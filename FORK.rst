TinyTorrent pause fix
====================

Why this fork exists
--------------------

TinyTorrent's Debug engine crashed when adding a magnet while Pause all was
active (TinyTorrent issue #140). Individual Resume followed by Pause while
Pause all remained active reproduced the same assertion. These are ordinary
libtorrent API operations; the inconsistency is in libtorrent's pause
bookkeeping.

This fork keeps the correction in Git so TinyTorrent can build an exact source
revision and retain it across upstream updates. It is based on upstream
``6da363d2994f17c0b3c0450d124cf73a31a73847`` (v2.1.2). The maintained branch is
``codex/paused-tick``. The correction changes only ``src/torrent.cpp``; these
fork notes and the README link are documentation. No upstream pull request is
planned.

Confirmed reproduction
----------------------

Use a TinyTorrent Debug engine linked against the unmodified upstream baseline,
with assertions enabled and an empty disposable download directory. A real
transfer is not required.

1. Turn on Pause all.
2. Add this trackerless magnet, choosing to add it paused::

      magnet:?xt=urn:btih:0123456789012345678901234567890123456789

3. Leave the engine running for at least three seconds so its tick runs.

Expected: the torrent is accepted, Pause all remains active, and the engine
stays alive. Observed on the baseline: the Debug engine aborts on
``TORRENT_ASSERT(t.want_tick())`` in ``session_impl.cpp:3754``.

A second confirmed sequence avoids adding during Pause all:

1. Add the same magnet paused while Pause all is off.
2. Turn on Pause all.
3. Resume that individual torrent, then pause it again without clearing
   Pause all.
4. Wait at least three seconds. The baseline hits the same assertion.

At the library boundary, the second sequence is: add a non-auto-managed paused
torrent, call ``session::pause()``, then ``torrent_handle::resume()`` and
``torrent_handle::pause()`` for that torrent. Keep the session alive for the
next tick. These operations are serialized by libtorrent's session thread.

The confirmed failure is a Debug assertion. A short Release probe did not
crash; a Release crash has not been established.

Cause and correction
--------------------

Effective pause is the combination of the torrent's own pause flag and the
session pause flag. ``torrent::set_paused()`` changed the individual flag and
returned early when session pause kept the effective state unchanged. But
``want_tick()`` also depends on the individual flag: a peerless torrent could
remain in the tick list after it no longer wanted ticks. The next session tick
asserted. Scrape membership, gauges, and state notifications also depend on
individual intent.

The correction updates that bookkeeping even when effective pause does not
change. The related resume path refreshes scrape membership and state
notifications too. Review also found overlapping graceful-pause cases:

* Final-peer completion must retain the torrent's own flag instead of replacing
  a Resume request with Pause.
* Effective resume must clear graceful mode even when it follows session resume.
* An ordinary hard Pause must finish graceful draining even when session pause
  hides the effective transition.

These cases stay in the existing pause owners. The fork does not disable
assertions, briefly resume the session, or introduce a second pause mechanism.
An existing extension-hook override inconsistency is outside this correction;
arbitrary ``on_pause``/``on_resume`` interception is not claimed fixed.

Verification as of 2026-10-07
-----------------------------

The initial tick correction, ``21aec1b``, passed TinyTorrent's Debug magnet-add
and individual Resume/Pause checks, including restart persistence. Subsequent
bookkeeping and graceful-pause changes were reviewed twice normally and twice
adversarially, with follow-up review of the final code at ``627695b``.

Upstream tests built against ``627695b`` in Windows x64 Debug, with assertions
and invariant checks enabled, produced these results:

* Native suite: 113 of 114 executables passed. ``test_upnp`` failed three
  callback-count assertions in ``upnp_wipconn`` and failed the same way when
  run alone. Its cause is unconfirmed; it is not a clean full-suite pass.
* Simulations: 10 executables passed, including all eight existing pause cases,
  auto-management, torrent status, and session tests. Three transfer-matrix
  executables exceeded the runner's 400-second limit. Their longer retry and
  the remaining build/tests were stopped at the owner's request.

The simulations are incomplete. Existing pause tests do not cover all the
individual-intent and graceful-drain interleavings corrected here, and no new
upstream regression cases have been added. The latest upstream test binaries
do not replace TinyTorrent's installed dependency libraries.

Maintenance
-----------

TinyTorrent pins the fork's tested code commit in ``engine/src/Dependencies.ps1``;
its ignored ``3rdParty/libtorrent`` directory is a checkout, not the durable
record of the change. Merge upstream changes into this fork, review whether
the correction is still needed, and update TinyTorrent's pin only deliberately.
The original upstream repository remains ``arvidn/libtorrent``. Remove the
source deviation once upstream preserves these pause invariants and the
reproduction sequences succeed there.
