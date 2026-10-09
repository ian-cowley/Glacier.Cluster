namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;

public partial struct NodeHardwareProfile
{
    /// <summary>
    /// Performs dynamic self-probing of the local machine hardware topology.
    /// Safely inspects CPU, SIMD, RAM, GPU, and Network without throwing exceptions.
    /// </summary>
    /// <param name="nodeId">Optional explicit NodeId; if null, resolved from environment or generated.</param>
    /// <param name="hostIpv4">Optional explicit host IPv4 uint; if null, probed from network adapters.</param>
    /// <returns>Populated 64-byte NodeHardwareProfile struct.</returns>
    public static NodeHardwareProfile ProbeLocal(Guid? nodeId = null, uint? hostIpv4 = null)
    {
        // 1. Resolve NodeId
        Guid id = nodeId ?? ResolveNodeId();

        // 2. Probe CPU Cores & SIMD
        var (cpuCores, simdFlags) = ProbeCpu();

        // 3. Probe System RAM
        var (ramTotal, ramAvail) = ProbeRam();

        // 4. Probe GPUs & VRAM
        var (gpuFlags, vramTotal, vramAvail) = ProbeGpus();

        // 5. Probe Host IPv4
        uint ip = hostIpv4 ?? ProbeLocalIpv4();

        return new NodeHardwareProfile
        {
            NodeId = id,
            CpuLogicalCores = cpuCores,
            SimdFlags = simdFlags,
            GpuFlags = gpuFlags,
            Reserved1 = ip,
            RamTotalBytes = ramTotal,
            RamAvailableBytes = ramAvail,
            VramTotalBytes = vramTotal,
            VramAvailableBytes = vramAvail
        };
    }

    private static Guid ResolveNodeId()
    {
        try
        {
            string? envId = Environment.GetEnvironmentVariable("GLACIER_NODE_ID");
            if (!string.IsNullOrWhiteSpace(envId) && Guid.TryParse(envId, out Guid parsed))
            {
                return parsed;
            }
        }
        catch
        {
            // Fallback to new Guid
        }

        return Guid.NewGuid();
    }

    private static (uint cores, SimdArchitectureFlags flags) ProbeCpu()
    {
        uint cores = (uint)Math.Max(1, Environment.ProcessorCount);
        var simd = SimdArchitectureFlags.None;

        // x86 / x64 Vector Intrinsics
        if (RuntimeInformation.ProcessArchitecture is Architecture.X64 or Architecture.X86)
        {
            if (Sse42.IsSupported)
                simd |= SimdArchitectureFlags.Sse42;

            if (Avx.IsSupported)
                simd |= SimdArchitectureFlags.Avx;

            if (Avx2.IsSupported || Vector256.IsHardwareAccelerated)
                simd |= SimdArchitectureFlags.Avx2;

            if (Avx512F.IsSupported || Vector512.IsHardwareAccelerated)
                simd |= SimdArchitectureFlags.Avx512;
        }
        // ARM64 Vector Intrinsics
        else if (RuntimeInformation.ProcessArchitecture == Architecture.Arm64)
        {
            if (AdvSimd.IsSupported)
                simd |= SimdArchitectureFlags.AdvSimd;

            if (Sve.IsSupported)
                simd |= SimdArchitectureFlags.Sve;
        }

        return (cores, simd);
    }

    private static (ulong totalBytes, ulong availBytes) ProbeRam()
    {
        try
        {
            if (OperatingSystem.IsWindows())
            {
                var mem = new MEMORYSTATUSEX { dwLength = (uint)Marshal.SizeOf<MEMORYSTATUSEX>() };
                if (GlobalMemoryStatusEx(ref mem))
                {
                    return (mem.ullTotalPhys, mem.ullAvailPhys);
                }
            }
            else if (OperatingSystem.IsLinux() && File.Exists("/proc/meminfo"))
            {
                ulong memTotal = 0;
                ulong memAvail = 0;

                foreach (string line in File.ReadLines("/proc/meminfo"))
                {
                    if (line.StartsWith("MemTotal:", StringComparison.OrdinalIgnoreCase))
                    {
                        memTotal = ParseMemInfoKb(line) * 1024UL;
                    }
                    else if (line.StartsWith("MemAvailable:", StringComparison.OrdinalIgnoreCase))
                    {
                        memAvail = ParseMemInfoKb(line) * 1024UL;
                    }

                    if (memTotal > 0 && memAvail > 0)
                        break;
                }

                if (memTotal > 0)
                {
                    return (memTotal, memAvail > 0 ? memAvail : memTotal / 2);
                }
            }
        }
        catch
        {
            // Graceful fallback to GC memory telemetry
        }

        // Universal Fallback
        try
        {
            var gc = GC.GetGCMemoryInfo();
            ulong total = (ulong)gc.TotalAvailableMemoryBytes;
            if (total == 0) total = 8UL * 1024 * 1024 * 1024; // 8GB default if undetermined
            ulong used = (ulong)Environment.WorkingSet;
            ulong avail = total > used ? total - used : total / 2;
            return (total, avail);
        }
        catch
        {
            return (8UL * 1024 * 1024 * 1024, 4UL * 1024 * 1024 * 1024);
        }
    }

    private static ulong ParseMemInfoKb(string line)
    {
        int start = line.IndexOf(':');
        if (start < 0) return 0;
        string valStr = line[(start + 1)..].Trim();
        int end = valStr.IndexOf(' ');
        if (end > 0) valStr = valStr[..end];
        return ulong.TryParse(valStr, out ulong kb) ? kb : 0;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GlobalMemoryStatusEx(ref MEMORYSTATUSEX lpBuffer);

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
    private struct MEMORYSTATUSEX
    {
        public uint dwLength;
        public uint dwMemoryLoad;
        public ulong ullTotalPhys;
        public ulong ullAvailPhys;
        public ulong ullTotalPageFile;
        public ulong ullAvailPageFile;
        public ulong ullTotalVirtual;
        public ulong ullAvailVirtual;
        public ulong ullAvailExtendedVirtual;
    }

    private static (GpuBackendFlags flags, ulong vramTotal, ulong vramAvail) ProbeGpus()
    {
        var flags = GpuBackendFlags.None;
        ulong totalVram = 0;
        ulong availVram = 0;

        // 1. Probe NVIDIA CUDA
        if (TryProbeCuda(out ulong cudaTotal, out ulong cudaAvail))
        {
            flags |= GpuBackendFlags.Cuda;
            totalVram = Math.Max(totalVram, cudaTotal);
            availVram = Math.Max(availVram, cudaAvail);
        }

        // 2. Probe AMD ROCm / HIP
        if (TryProbeRocm(out ulong rocmTotal, out ulong rocmAvail))
        {
            flags |= GpuBackendFlags.Rocm;
            totalVram = Math.Max(totalVram, rocmTotal);
            availVram = Math.Max(availVram, rocmAvail);
        }

        // 3. Probe Direct3D 12 (Windows only)
        if (OperatingSystem.IsWindows() && TryProbeD3D12(out ulong d3dTotal, out ulong d3dAvail))
        {
            flags |= GpuBackendFlags.Direct3D12;
            totalVram = Math.Max(totalVram, d3dTotal);
            availVram = Math.Max(availVram, d3dAvail);
        }

        // 4. Probe Vulkan (Cross-Platform)
        if (TryProbeVulkan(out ulong vkTotal, out ulong vkAvail))
        {
            flags |= GpuBackendFlags.Vulkan;
            totalVram = Math.Max(totalVram, vkTotal);
            availVram = Math.Max(availVram, vkAvail);
        }

        // 5. Probe Apple Metal (macOS only)
        if (OperatingSystem.IsMacOS() && TryProbeMetal(out ulong metalTotal, out ulong metalAvail))
        {
            flags |= GpuBackendFlags.Metal;
            totalVram = Math.Max(totalVram, metalTotal);
            availVram = Math.Max(availVram, metalAvail);
        }

        return (flags, totalVram, availVram);
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int CuInitDelegate(int flags);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int CuDeviceGetCountDelegate(out int count);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int CuDeviceGetDelegate(out int device, int ordinal);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int CuDeviceTotalMemDelegate(out nuint bytes, int dev);

    private static bool TryProbeCuda(out ulong totalVram, out ulong availVram)
    {
        totalVram = 0;
        availVram = 0;

        string libName = OperatingSystem.IsWindows() ? "nvcuda.dll" : "libcuda.so.1";
        if (!NativeLibrary.TryLoad(libName, out IntPtr handle))
        {
            if (OperatingSystem.IsLinux() && !NativeLibrary.TryLoad("libcuda.so", out handle))
            {
                return false;
            }
            if (handle == IntPtr.Zero) return false;
        }

        try
        {
            if (NativeLibrary.TryGetExport(handle, "cuInit", out IntPtr cuInitPtr) &&
                NativeLibrary.TryGetExport(handle, "cuDeviceGetCount", out IntPtr cuGetCountPtr))
            {
                var cuInit = Marshal.GetDelegateForFunctionPointer<CuInitDelegate>(cuInitPtr);
                var cuGetCount = Marshal.GetDelegateForFunctionPointer<CuDeviceGetCountDelegate>(cuGetCountPtr);

                if (cuInit(0) == 0 && cuGetCount(out int count) == 0 && count > 0)
                {
                    IntPtr cuTotalMemPtr = IntPtr.Zero;
                    _ = NativeLibrary.TryGetExport(handle, "cuDeviceTotalMem_v2", out cuTotalMemPtr) ||
                        NativeLibrary.TryGetExport(handle, "cuDeviceTotalMem", out cuTotalMemPtr);

                    if (cuTotalMemPtr != IntPtr.Zero &&
                        NativeLibrary.TryGetExport(handle, "cuDeviceGet", out IntPtr cuDeviceGetPtr))
                    {
                        var cuDeviceGet = Marshal.GetDelegateForFunctionPointer<CuDeviceGetDelegate>(cuDeviceGetPtr);
                        var cuTotalMem = Marshal.GetDelegateForFunctionPointer<CuDeviceTotalMemDelegate>(cuTotalMemPtr);

                        if (cuDeviceGet(out int dev, 0) == 0 && cuTotalMem(out nuint memBytes, dev) == 0)
                        {
                            totalVram = (ulong)memBytes;
                            availVram = (ulong)(memBytes * 0.90);
                            return true;
                        }
                    }

                    // Device exists even if memory query fails
                    totalVram = 4UL * 1024 * 1024 * 1024;
                    availVram = 3UL * 1024 * 1024 * 1024;
                    return true;
                }
            }
        }
        catch
        {
            // Silently handle any CUDA driver interop failure
        }
        finally
        {
            NativeLibrary.Free(handle);
        }

        return false;
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int HipInitDelegate(int flags);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int HipGetDeviceCountDelegate(out int count);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int HipDeviceTotalMemDelegate(out nuint bytes, int device);

    private static bool TryProbeRocm(out ulong totalVram, out ulong availVram)
    {
        totalVram = 0;
        availVram = 0;

        string libName = OperatingSystem.IsWindows() ? "amdhip64.dll" : "libamdhip64.so";
        if (!NativeLibrary.TryLoad(libName, out IntPtr handle))
        {
            if (OperatingSystem.IsLinux() && !NativeLibrary.TryLoad("libhip64.so", out handle))
            {
                return false;
            }
            if (handle == IntPtr.Zero) return false;
        }

        try
        {
            if (NativeLibrary.TryGetExport(handle, "hipInit", out IntPtr initPtr) &&
                NativeLibrary.TryGetExport(handle, "hipGetDeviceCount", out IntPtr countPtr))
            {
                var hipInit = Marshal.GetDelegateForFunctionPointer<HipInitDelegate>(initPtr);
                var hipCount = Marshal.GetDelegateForFunctionPointer<HipGetDeviceCountDelegate>(countPtr);

                if (hipInit(0) == 0 && hipCount(out int count) == 0 && count > 0)
                {
                    if (NativeLibrary.TryGetExport(handle, "hipDeviceTotalMem", out IntPtr memPtr))
                    {
                        var hipMem = Marshal.GetDelegateForFunctionPointer<HipDeviceTotalMemDelegate>(memPtr);
                        if (hipMem(out nuint memBytes, 0) == 0)
                        {
                            totalVram = (ulong)memBytes;
                            availVram = (ulong)(memBytes * 0.90);
                            return true;
                        }
                    }

                    totalVram = 4UL * 1024 * 1024 * 1024;
                    availVram = 3UL * 1024 * 1024 * 1024;
                    return true;
                }
            }
        }
        catch
        {
            // Silently handle ROCm error
        }
        finally
        {
            NativeLibrary.Free(handle);
        }

        return false;
    }

    private static bool TryProbeD3D12(out ulong totalVram, out ulong availVram)
    {
        totalVram = 0;
        availVram = 0;

        if (!NativeLibrary.TryLoad("d3d12.dll", out IntPtr d3d12Handle))
            return false;

        NativeLibrary.Free(d3d12Handle);

        if (!NativeLibrary.TryLoad("dxgi.dll", out IntPtr dxgiHandle))
            return false;

        NativeLibrary.Free(dxgiHandle);

        // Direct3D 12 and DXGI libraries are present and operational on Windows
        // Default estimate if not queried via full COM interface
        totalVram = 4UL * 1024 * 1024 * 1024;
        availVram = 3UL * 1024 * 1024 * 1024;
        return true;
    }

    private static bool TryProbeVulkan(out ulong totalVram, out ulong availVram)
    {
        totalVram = 0;
        availVram = 0;

        string libName = OperatingSystem.IsWindows() ? "vulkan-1.dll" :
                         OperatingSystem.IsMacOS() ? "libMoltenVK.dylib" : "libvulkan.so.1";

        if (!NativeLibrary.TryLoad(libName, out IntPtr handle))
        {
            if (OperatingSystem.IsLinux() && !NativeLibrary.TryLoad("libvulkan.so", out handle))
            {
                return false;
            }
            if (handle == IntPtr.Zero) return false;
        }

        try
        {
            if (NativeLibrary.TryGetExport(handle, "vkGetInstanceProcAddr", out _))
            {
                totalVram = 4UL * 1024 * 1024 * 1024;
                availVram = 3UL * 1024 * 1024 * 1024;
                return true;
            }
        }
        catch
        {
            // Silently handle Vulkan error
        }
        finally
        {
            NativeLibrary.Free(handle);
        }

        return false;
    }

    private static bool TryProbeMetal(out ulong totalVram, out ulong availVram)
    {
        totalVram = 0;
        availVram = 0;

        if (!OperatingSystem.IsMacOS())
            return false;

        if (NativeLibrary.TryLoad("/System/Library/Frameworks/Metal.framework/Metal", out IntPtr handle))
        {
            NativeLibrary.Free(handle);
            totalVram = 8UL * 1024 * 1024 * 1024;
            availVram = 6UL * 1024 * 1024 * 1024;
            return true;
        }

        return false;
    }

    private static uint ProbeLocalIpv4()
    {
        try
        {
            var interfaces = NetworkInterface.GetAllNetworkInterfaces();

            // Pass 1: Look for operational physical Ethernet / Wi-Fi on 192.168.1.x subnet
            foreach (var ni in interfaces)
            {
                if (ni.OperationalStatus != OperationalStatus.Up)
                    continue;
                if (ni.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                    continue;

                foreach (var ua in ni.GetIPProperties().UnicastAddresses)
                {
                    if (ua.Address.AddressFamily == AddressFamily.InterNetwork)
                    {
                        byte[] bytes = ua.Address.GetAddressBytes();
                        if (bytes[0] == 192 && bytes[1] == 168 && bytes[2] == 1)
                        {
                            return BinaryPrimitives.ReadUInt32BigEndian(bytes);
                        }
                    }
                }
            }

            // Pass 2: Any operational non-loopback IPv4 address
            foreach (var ni in interfaces)
            {
                if (ni.OperationalStatus != OperationalStatus.Up)
                    continue;
                if (ni.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                    continue;

                foreach (var ua in ni.GetIPProperties().UnicastAddresses)
                {
                    if (ua.Address.AddressFamily == AddressFamily.InterNetwork)
                    {
                        return BinaryPrimitives.ReadUInt32BigEndian(ua.Address.GetAddressBytes());
                    }
                }
            }
        }
        catch
        {
            // Silently ignore network enumeration errors
        }

        // Loopback 127.0.0.1 default
        return 0x7F000001;
    }
}
