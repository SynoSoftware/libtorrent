TinyTorrent libtorrent changes
==============================

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
``Synapse/TinyTorrent``. The pause correction changes only ``src/torrent.cpp``.
No upstream pull request is planned.

Known peer snapshots
--------------------

``torrent_handle::post_peer_info(true)`` includes ``known_peers`` in the usual
``peer_info_alert``. Each entry contains an IP endpoint, discovery sources and
the retained confirmed-seed flag. False does not prove that a peer is a leecher.
The collection includes disconnected and banned IP peers; I2P and RTC destinations
have no geographic IP location and are excluded. Live connections retain their
existing detailed records, including non-IP connections.

Both lists are copied on the session thread in the same callback. The optional
scan is linear in the retained list and does not connect, discover, or keep a
second peer store. The default call does not scan the known list. TinyTorrent
requests it only for the visible Peers inspector. All clients must be rebuilt
against the matching headers and library because the alert layout changed.

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

Storage that does not answer
----------------------------

Windows reports a network share or drive that does not answer with
``ERROR_BAD_NETPATH``, ``ERROR_BAD_NET_NAME`` or ``ERROR_PATH_NOT_FOUND``.
Boost maps all three to ``no_such_file_or_directory``, and libtorrent treats
that as a file with no data yet. A check that runs while a share is absent
therefore finishes at 0% without an error, and the next resume data saves
that result over the real progress. A rename records the new name without
renaming the file.

The fork keeps the rule that a missing file has no data, but only while the
save path answers:

* ``check_storage_root()`` replaces a missing-file error with
  ``ERROR_DEV_NOT_EXIST`` when neither the save path nor its nearest existing
  parent answers. The walk stops at the root (``C:\`` or
  ``\\server\share\``), so a save folder not created yet on an answering share
  still holds files with no data. It opens each folder instead of reading its
  attributes, because only opening follows a save path that is a link or
  junction to its target. A link that exists but cannot be followed, such as
  one to a removed volume, does not answer. The stat cache, file opens and
  hash reads in the pread storage, file opens in the mmap storage,
  ``rename_file``, ``move_storage`` and ``part_file::move_partfile`` call it,
  so checks, writes, renames and moves report a disk error. ``move_storage``
  tests both its source and its destination, because a file that either end
  cannot reach would otherwise count as a missing source and stay behind.
* The pread storage's ``initialize()`` fails instead of opening a part file
  from a save path that does not answer, because that part file reads as
  empty and its first write replaces the pieces it holds.
  ``set_file_priority()`` reports the error instead of treating such a file as
  missing.
* ``check_fastresume`` without resume data reports the error instead of
  treating ``has_any_file()`` as false, which skipped the check and downloaded
  existing files again.
* ``move_storage`` with ``reset_save_path`` or ``reset_save_path_unchecked``
  succeeds when the destination does not answer, because it moves no files.
  The first write creates the folder, and the check reports the error.
* A check of resume data or the setup of a recheck that fails with a torrent
  error runs again when the error clears. A failed check of resume data keeps
  ``m_add_torrent_params``, and ``write_resume_data`` saves its pieces
  unchanged until that check succeeds.
* The pread storage hashes a piece before it writes its blocks, so the piece
  already counts as had when a write fails. ``on_counted_piece_unwritten()``
  then recounts the file progress from the picker, which ``write_failed`` has
  corrected, so a file is not reported complete while its data is missing,
  and a torrent that finished with that piece resumes downloading. A seed has
  no picker, so it checks all its files when ``clear_error()`` runs, and
  ``write_resume_data`` saves no pieces until then. A ``force_recheck()`` in
  the meantime is that check, so only one check starts.
* A piece whose blocks were written but could not be read back for its hash
  check is cleared and downloaded again, as after a failed write, because
  nothing hashes a finished piece again.
* A check that a disk error stops keeps its position at the lowest piece
  whose hash has not returned, instead of starting again from the first
  piece. Hash jobs complete out of order, so that piece can be below the one
  that failed. The reads still in flight belong to the stopped pass, so their
  completions are ignored and their pieces are hashed again. Clearing the
  error continues the check there, and resume data saved before then lists
  the pieces checked before it, so a reloaded torrent continues the check
  after them. Resume data saved while the check runs ends its checked pieces
  at the same lowest piece, so a reloaded torrent hashes the pieces that were
  in flight.

Not yet built or tested: TinyTorrent builds the dependencies separately, and
the change awaits that build and a manual check with a share that is
disconnected and reconnected.

Maintenance
-----------

TinyTorrent pins the fork's tested code commit in ``engine/src/Dependencies.ps1``;
its ignored ``3rdParty/libtorrent`` directory is a checkout, not the durable
record of the change. Merge upstream changes into this fork, review whether
the correction is still needed, and update TinyTorrent's pin only deliberately.
The original upstream repository remains ``arvidn/libtorrent``. Remove the
source deviation once upstream preserves these pause invariants and the
reproduction sequences succeed there.
