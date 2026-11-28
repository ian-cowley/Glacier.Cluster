namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using Glacier.Inference.Gpu;
using Glacier.Inference.Gpu.D3D12;

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

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool GlobalMemoryStatusEx(ref MEMORYSTATUSEX lpBuffer);

    [StructLayout(LayoutKind.Sequential)]
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

    private static readonly Lazy<(GpuBackendFlags flags, ulong vramTotal, ulong vramAvail)> s_cachedGpuProbe = new(ProbeGpusCore);

    private static (GpuBackendFlags flags, ulong vramTotal, ulong vramAvail) ProbeGpus()
        => s_cachedGpuProbe.Value;

    private static (GpuBackendFlags flags, ulong vramTotal, ulong vramAvail) ProbeGpusCore()
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

                    // Device enumerated, but VRAM query symbol/call unavailable: report honest 0 instead of fabricated size
                    totalVram = 0;
                    availVram = 0;
                    return true;
                }
            }
        }
        catch (Exception ex)
        {
            Trace.TraceWarning($"[NodeHardwareProfile] CUDA interop probe warning: {ex.Message}");
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

                    totalVram = 0;
                    availVram = 0;
                    return true;
                }
            }
        }
        catch (Exception ex)
        {
            Trace.TraceWarning($"[NodeHardwareProfile] ROCm interop probe warning: {ex.Message}");
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

        if (!OperatingSystem.IsWindows())
            return false;

        try
        {
            var adapters = D3D12Context.EnumerateAdapters();
            if (adapters.Count == 0)
                return false;

            ulong bestVram = 0;
            foreach (var adapter in adapters)
            {
                ulong effective = adapter.DedicatedVideoMemory > 0
                    ? adapter.DedicatedVideoMemory
                    : adapter.SharedSystemMemory;
                if (effective > bestVram)
                {
                    bestVram = effective;
                }
            }

            totalVram = bestVram;
            availVram = (ulong)(bestVram * 0.90);
            return true;
        }
        catch (Exception ex)
        {
            Trace.TraceWarning($"[NodeHardwareProfile] D3D12 DXGI probe warning: {ex.Message}");
            return false;
        }
    }

    private static bool TryProbeVulkan(out ulong totalVram, out ulong availVram)
    {
        totalVram = 0;
        availVram = 0;

        try
        {
            var devices = VulkanContext.EnumerateDevices();
            if (devices.Count == 0)
                return false;

            ulong bestVram = 0;
            foreach (var dev in devices)
            {
                if (dev.TotalMemoryBytes > bestVram)
                {
                    bestVram = dev.TotalMemoryBytes;
                }
            }

            totalVram = bestVram;
            availVram = (ulong)(bestVram * 0.90);
            return true;
        }
        catch (Exception ex)
        {
            Trace.TraceWarning($"[NodeHardwareProfile] Vulkan device probe warning: {ex.Message}");
            return false;
        }
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
            // Without Objective-C MTLCreateSystemDefaultDevice memory query, report 0 instead of stubbed constant.
            totalVram = 0;
            availVram = 0;
            return true;
        }

        return false;
    }

    private static uint ProbeLocalIpv4()
    {
        try
        {
            var interfaces = NetworkInterface.GetAllNetworkInterfaces();
            uint? gatewayPrivateIpv4 = null;
            uint? anyGatewayIpv4 = null;
            uint? rfc1918Ipv4 = null;
            uint? fallbackIpv4 = null;

            foreach (var ni in interfaces)
            {
                if (ni.OperationalStatus != OperationalStatus.Up)
                    continue;
                if (ni.NetworkInterfaceType is NetworkInterfaceType.Loopback or NetworkInterfaceType.Tunnel)
                    continue;

                var ipProps = ni.GetIPProperties();
                bool hasGateway = ipProps.GatewayAddresses.Count > 0;

                foreach (var ua in ipProps.UnicastAddresses)
                {
                    if (ua.Address.AddressFamily != AddressFamily.InterNetwork)
                        continue;

                    byte[] bytes = ua.Address.GetAddressBytes();
                    // Skip APIPA (169.254.x.x) and loopback (127.x.x.x)
                    if ((bytes[0] == 169 && bytes[1] == 254) || bytes[0] == 127)
                        continue;

                    uint addr = BinaryPrimitives.ReadUInt32BigEndian(bytes);
                    bool isPrivate = bytes[0] == 10 ||
                                     (bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31) ||
                                     (bytes[0] == 192 && bytes[1] == 168) ||
                                     (bytes[0] == 100 && bytes[1] >= 64 && bytes[1] <= 127);

                    if (hasGateway)
                    {
                        if (isPrivate)
                        {
                            gatewayPrivateIpv4 ??= addr;
                        }
                        else
                        {
                            anyGatewayIpv4 ??= addr;
                        }
                    }
                    else if (isPrivate)
                    {
                        rfc1918Ipv4 ??= addr;
                    }
                    else
                    {
                        fallbackIpv4 ??= addr;
                    }
                }
            }

            if (gatewayPrivateIpv4.HasValue) return gatewayPrivateIpv4.Value;
            if (anyGatewayIpv4.HasValue) return anyGatewayIpv4.Value;
            if (rfc1918Ipv4.HasValue) return rfc1918Ipv4.Value;
            if (fallbackIpv4.HasValue) return fallbackIpv4.Value;
        }
        catch (NetworkInformationException ex)
        {
            Trace.TraceWarning($"[NodeHardwareProfile] Network interface enumeration warning: {ex.Message}");
        }

        // Loopback 127.0.0.1 default
        return 0x7F000001;
    }
}
