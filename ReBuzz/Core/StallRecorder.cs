using System;
using System.Diagnostics;
using System.Threading;

namespace ReBuzz.Core
{
    // ─────────────────────────────────────────────────────────────────────────
    // Stage-2 stall recorder (engine side).
    //
    // At each ASIO deadline miss, captures the worst machine Work() in a recent
    // look-back window, plus the load shape, so PP2 can accumulate across many
    // misses and name the invariant cause (SLOW_MACHINE / OVERLOAD / STARVE).
    //
    // Zero observer effect when disarmed: the ONLY cost added to the audio path
    // while _armed == false is a single volatile-bool read in TickAndWork. When
    // armed, per-Work cost is two Stopwatch reads + one lock-free ring store — no
    // allocation, no locks. The miss handler (rare) allocates nothing: the
    // snapshot ring is pre-allocated and mutated in place.
    //
    // Clock: everything uses raw Stopwatch.GetTimestamp() (QPC), so WorkRec end
    // times and the miss time share one monotonic axis.
    // ─────────────────────────────────────────────────────────────────────────
    public static class StallRecorder
    {
        // ── arm flag (PP2 drives this via ReBuzzCore.StallCaptureArmed) ─────
        private static volatile bool _armed;
        public static bool Armed => _armed;
        public static void SetArmed(bool on) => _armed = on;

        // ── config context, set by ReBuzzCore at engine start ───────────────
        public static int CtxAudioThreads = 8;   // used by PP2's OVERLOAD test; never leave 0
        public static int CtxFillThread   = 1;

        // ── tunables ────────────────────────────────────────────────────────
        public static double WindowMs      = 50.0;   // look-back before the miss
        private const int    DWELL_FLOOR_US = 1500;  // a Work must exceed this to count as "heavy"
        private static readonly double _qpcHz = Stopwatch.Frequency;

        // ── WorkRec ring (multi-producer, lock-free) ────────────────────────
        private struct WorkRec { public long EndQpc; public MachineCore Machine; public int DwellUs; }
        private const int WORK_RING = 1 << 14;               // 16384 recent Works (~power of two)
        private static readonly WorkRec[] _work = new WorkRec[WORK_RING];
        private static long _workIdx;                        // monotonic write index

        // GC Gen2 proximity (armed path). Stamped when a Gen2 completes.
        private static int  _lastG2Count = -1;
        private static long _lastG2Qpc;

        /// <summary>WORKER THREADS. Called from MachineWorkInstance.TickAndWork,
        /// armed only. start/end are raw Stopwatch.GetTimestamp() values.</summary>
        public static void OnWorkComplete(MachineCore machine, long startTicks, long endTicks)
        {
            long i = Interlocked.Increment(ref _workIdx) - 1;
            int slot = (int)(i & (WORK_RING - 1));
            long dwell = endTicks - startTicks;
            _work[slot].EndQpc  = endTicks;
            _work[slot].Machine = machine;
            _work[slot].DwellUs = (int)Math.Min(dwell * 1_000_000.0 / _qpcHz, int.MaxValue);

            // GC Gen2 proximity: stamp the QPC when a Gen2 completes. CAS so only
            // one worker records each increment; first observation (prev<0) seeds.
            int g2 = GC.CollectionCount(2);
            int prev = Volatile.Read(ref _lastG2Count);
            if (g2 != prev && Interlocked.CompareExchange(ref _lastG2Count, g2, prev) == prev && prev >= 0)
                Volatile.Write(ref _lastG2Qpc, endTicks);
        }

        // ── snapshot ring (written at the miss; PP2 drains) ─────────────────
        // Public DTO with public fields so PP2 reads it by reflection, exactly
        // like it already reads DeadlineMissCount.
        public sealed class StallSnapshotDto
        {
            public long   Qpc;
            public int    OverrunUs;
            public int    WindowMs;
            public string WorstMachine;    // machine name, resolved at the miss (cheap, rare)
            public int    WorstDwellUs;
            public int    WorksOverFloor;
            public int    TotalBusyUs;
            public int    AnyWorkRan;       // 0/1
            public string IpcWaitMachine;   // reserved for v2 (native IPC flag); v1 = null
            public int    IpcWaitUs;        // reserved for v2; v1 = 0
            public int    AudioThreads;
            public int    FillThread;
            public int    GcAgeMs;          // ms since last completed Gen2 at the miss (-1 = none seen)
        }

        private const int SNAP_RING = 256;
        private static readonly StallSnapshotDto[] _snaps = new StallSnapshotDto[SNAP_RING];
        private static long _snapIdx;

        static StallRecorder()
        {
            for (int i = 0; i < SNAP_RING; i++) _snaps[i] = new StallSnapshotDto();
        }

        // PP2 poll surface (exposed through ReBuzzCore pass-throughs).
        public static long StallSnapshotWriteIndex => Interlocked.Read(ref _snapIdx);
        public static StallSnapshotDto[] StallSnapshotRing => _snaps;

        /// <summary>AUDIO CALLBACK THREAD. Called from CommonAudioProvider at the
        /// deadline miss. `nowQpc` must be Stopwatch.GetTimestamp() (same clock as
        /// OnWorkComplete). No allocation: mutates a pre-allocated ring slot.</summary>
        public static void OnDeadlineMiss(long overrunUs, long nowQpc)
        {
            if (!_armed) return;

            long lo = nowQpc - (long)(WindowMs / 1000.0 * _qpcHz);

            long widx     = Interlocked.Read(ref _workIdx);
            long scanFrom = Math.Max(0, widx - WORK_RING);   // only slots still in the ring
            int  worstUs  = 0;   MachineCore worstM = null;
            long totalUs  = 0;   int overFloor = 0;   bool any = false;

            for (long k = scanFrom; k < widx; k++)
            {
                // torn reads possible (workers writing concurrently) — one noisy
                // sample in the window is acceptable, same as PP2's own rings.
                ref readonly var r = ref _work[(int)(k & (WORK_RING - 1))];
                long end = r.EndQpc;
                if (end < lo || end > nowQpc) continue;
                any = true;
                int d = r.DwellUs;
                totalUs += d;
                if (d > DWELL_FLOOR_US) overFloor++;
                if (d > worstUs) { worstUs = d; worstM = r.Machine; }
            }

            long i = Interlocked.Increment(ref _snapIdx) - 1;
            var dto = _snaps[(int)(i & (SNAP_RING - 1))];
            dto.Qpc            = nowQpc;
            dto.OverrunUs      = (int)Math.Min(overrunUs, int.MaxValue);
            dto.WindowMs       = (int)WindowMs;
            dto.WorstMachine   = worstM != null ? SafeName(worstM) : null;
            dto.WorstDwellUs   = worstUs;
            dto.WorksOverFloor = overFloor;
            dto.TotalBusyUs    = (int)Math.Min(totalUs, int.MaxValue);
            dto.AnyWorkRan     = any ? 1 : 0;
            dto.IpcWaitMachine = null;   // v1
            dto.IpcWaitUs      = 0;      // v1
            dto.AudioThreads   = CtxAudioThreads;
            dto.FillThread     = CtxFillThread;
            long __g2q = Volatile.Read(ref _lastG2Qpc);
            dto.GcAgeMs        = __g2q > 0 ? (int)Math.Min((nowQpc - __g2q) * 1000.0 / _qpcHz, int.MaxValue) : -1;
        }

        private static string SafeName(MachineCore m)
        {
            try { return m.Name; } catch { return "?"; }
        }

        public static void Reset()
        {
            Interlocked.Exchange(ref _workIdx, 0);
            Interlocked.Exchange(ref _snapIdx, 0);
            Interlocked.Exchange(ref _lastG2Count, -1);
            Interlocked.Exchange(ref _lastG2Qpc, 0);
        }
    }
}
