# Copyright 2014 Christoph Reiter <reiter.christoph@gmail.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import re
import shlex

from senf import bytes2fsn, fsn2bytes

from quodlibet import const
from quodlibet.util import print_d, print_w
from quodlibet.util.dprint import Colorise
from .tcpserver import BaseTCPServer, BaseTCPConnection

# FIXME
import sys
print(f'Python version: {sys.version}')

const.print_idx = 0
def debug_print(action, **kwargs):
    print(f' - {action} (#{const.print_idx})')
    for key, val in kwargs.items():
        print(f'  {key}={val}')
    const.print_idx += 1
    print()


def unimplemented(name):
    print()
    print(Colorise.red(f'UNIMPLEMENTED: {name}'))
    print()


class AckError(object):
    NOT_LIST = 1
    ARG = 2
    PASSWORD = 3
    PERMISSION = 4
    UNKNOWN = 5
    NO_EXIST = 50
    PLAYLIST_MAX = 51
    SYSTEM = 52
    PLAYLIST_LOAD = 53
    UPDATE_ALREADY = 54
    PLAYER_SYNC = 55
    EXIST = 56


class Permissions(object):
    PERMISSION_NONE = 0
    PERMISSION_READ = 1
    PERMISSION_ADD = 2
    PERMISSION_CONTROL = 4
    PERMISSION_ADMIN = 8
    PERMISSION_ALL = PERMISSION_NONE | \
                     PERMISSION_READ | \
                     PERMISSION_ADD | \
                     PERMISSION_CONTROL | \
                     PERMISSION_ADMIN


# See: https://www.musicpd.org/doc/html/protocol.html#tags
SAME_TAGS = [
    "Artist",
    "ArtistSort", # same as artist, but for sorting
    "Album",
    "AbumSort", # same as album, but for sorting
    "AlbumArtist", # on multi-artist albums: the main artist
    "AlbumArtistAort", # same as albumartist, but for sorting
    "Title",
    "Genre",
    "Composer", # the artist who composed the song
    "Performer", # the artist who performed the song
    "Comment", # a human-readable comment about this song
    "Label", # label: the name of the label or publisher
    "Work", # a work is a distinct intellectual or artistic creation, which can be expressed in the form of one or more audio recordings
    "Grouping", # used if the sound belongs to a larger category of sounds/music
    "musicbrainz_artistid",
    "musicbrainz_albumid",
    "musicbrainz_albumartistid",
    "musicbrainz_trackid",
    "musicbrainz_releasetrackid",
    "musicbrainz_workid",
]

TAG_MAPPING = {
    "date": "~year", # a 4-digit year
    "track": "tracknumber", # the decimal track number within the album
    "name": "title", # a name for this song. combined artist name - song title.
    "disc": "discnumber", # the decimal disc number in a multi-disc album
}

def format_tags(song):
    """Gives a tag list message for a song"""

    mapping = {}
    for mpd_key in SAME_TAGS:
        mapping[mpd_key] = song.comma(mpd_key.lower())  

    for mpd_key, ql_key in TAG_MAPPING.items():
        mapping[mpd_key] = song.comma(ql_key)

    return '\n'.join(f"{key}: {val}" for key, val in mapping.items() if val)


class ParseError(Exception):
    def __init__(self, *args, **kwargs):
        debug_print('ParseError', args=args, **kwargs)


def parse_command(line):
    """Parses a MPD command (without trailing newline)

    Returns (command, [arguments]) or raises ParseError in case of an error.
    """

    assert isinstance(line, bytes)

    parts = re.split(b"[ \\t]+", line, maxsplit=1)
    if not parts:
        raise ParseError("empty command")
    command = parts[0]

    if len(parts) > 1:
        lex = shlex.shlex(bytes2fsn(parts[1], "utf-8"), posix=True)
        lex.whitespace_split = True
        lex.commenters = ""
        lex.quotes = "\""
        lex.whitespace = " \t"
        args = [fsn2bytes(a, "utf-8") for a in lex]
    else:
        args = []

    try:
        command = command.decode("utf-8")
    except ValueError as e:
        raise ParseError(e)

    dec_args = []
    for arg in args:
        try:
            arg = arg.decode("utf-8")
        except ValueError as e:
            raise ParseError(e)
        dec_args.append(arg)

    return command, dec_args


class MPDService(object):
    """This is the actual shared MPD service which the clients talk to"""

    version = (0, 22, 0)

    def __init__(self, app, config):
        self._app = app
        self._connections = set()
        self._idle_subscriptions = {}
        self._idle_queue = {}
        self._pl_ver = 0

        self._config = config
        self._options = app.player_options

        if not self._config.config_get("password"):
            self.default_permission = Permissions.PERMISSION_ALL
        else:
            self.default_permission = Permissions.PERMISSION_NONE

        def options_changed(*args):
            self.emit_changed("options")

        self._options.connect("notify::shuffle", options_changed)
        self._options.connect("notify::repeat", options_changed)
        self._options.connect("notify::single", options_changed)

        self._player_sigs = []

        def volume_changed(*args):
            self.emit_changed("mixer")

        id_ = app.player.connect("notify::volume", volume_changed)
        self._player_sigs.append(id_)

        def player_changed(*args):
            self.emit_changed("player")

        id_ = app.player.connect("paused", player_changed)
        self._player_sigs.append(id_)
        id_ = app.player.connect("unpaused", player_changed)
        self._player_sigs.append(id_)
        id_ = app.player.connect("seek", player_changed)
        self._player_sigs.append(id_)

        def playlist_changed(*args):
            self._pl_ver += 1
            self.emit_changed("playlist")

        id_ = app.player.connect("song-started", playlist_changed)
        self._player_sigs.append(id_)

    def _get_id(self, info):
        # XXX: we need a unique 31 bit ID, but don't have one.
        # Given that the heap is continuous and each object is >16 bytes
        # this should work
        return (id(info) & 0xFFFFFFFF) >> 1

    def destroy(self):
        for id_ in self._player_sigs:
            self._app.player.disconnect(id_)
        del self._options
        del self._app

    def add_connection(self, connection):
        self._connections.add(connection)
        self._idle_queue[connection] = set()

    def remove_connection(self, connection):
        self._idle_subscriptions.pop(connection, None)
        self._idle_queue.pop(connection, None)
        self._connections.remove(connection)

    def register_idle(self, connection, subsystems):
        self._idle_subscriptions[connection] = set(subsystems)
        self.flush_idle()

    def flush_idle(self):
        flushed = []
        for conn, subs in self._idle_subscriptions.items():
            # figure out which subsystems to report for each connection
            queued = self._idle_queue[conn]
            if subs:
                to_send = subs & queued
            else:
                to_send = queued
            queued -= to_send

            # send out the response and remove the idle status for affected
            # connections
            for subsystem in to_send:
                conn.write_line("changed: %s" % subsystem)
            if to_send:
                flushed.append(conn)
                # FIXME: why ok here?
                conn.ok()
                conn.start_write()

        for conn in flushed:
            self._idle_subscriptions.pop(conn, None)

    def unregister_idle(self, connection):
        self._idle_subscriptions.pop(connection, None)

    def emit_changed(self, subsystem):
        for _conn, subs in self._idle_queue.items():
            subs.add(subsystem)
        self.flush_idle()

    def play(self):
        self._app.player.playpause()

    def playid(self, songid):
        self.play()

    def pause(self, value=None):
        if value is None:
            self._app.player.paused = not self._app.player.paused
        else:
            self._app.player.paused = value

    def stop(self):
        self._app.player.stop()

    def next(self):
        self._app.player.next()

    def previous(self):
        self._app.player.previous()

    def seek(self, songpos, time_):
        """time_ in seconds"""

        self._app.player.seek(time_ * 1000)

    def seekid(self, songid, time_):
        """time_ in seconds"""

        self._app.player.seek(time_ * 1000)

    def seekcur(self, value, relative):
        if relative:
            pos = self._app.player.get_position()
            self._app.player.seek(pos + value * 1000)
        else:
            self._app.player.seek(value * 1000)

    def setvol(self, value):
        """value: 0..100"""

        self._app.player.volume = value / 100.0

    def repeat(self, value):
        self._options.repeat = value

    def random(self, value):
        self._options.shuffle = value

    def single(self, value):
        self._options.single = value

    def stats(self):
        app = self._app
        albums = list(app.library.albums.itervalues())

        song_count = 0
        artists = set()
        for album in albums:
            # print(album)
            # print(album('artist'))
            for song in album.songs:
                # print(song)
                artists.add(song('artist'))
                song_count += 1

        stats = [
            ("artists", len(artists)),
            ("albums", len(albums)),
            ("songs", song_count),
            ("uptime", 1),
            ("playtime", 1),
            ("db_playtime", 1),
            ("db_update", 1252868674),
        ]

        return stats

    def status(self):
        app = self._app
        info = app.player.info

        if info:
            if app.player.paused:
                state = "pause"
            else:
                state = "play"
        else:
            state = "stop"

        """
        volume: 0-100 (deprecated: -1 if the volume cannot be determined)
        repeat: 0 or 1
        random: 0 or 1
        single [2]: 0, 1, or oneshot [6]
        consume [2]: 0 or 1
        playlist: 31-bit unsigned integer, the playlist version number
        playlistlength: integer, the length of the playlist
        state: play, stop, or pause
        song: playlist song number of the current song stopped on or playing
        songid: playlist songid of the current song stopped on or playing
        nextsong [2]: playlist song number of the next song to be played
        nextsongid [2]: playlist songid of the next song to be played
        time: total time elapsed (of current playing/paused song) in seconds (deprecated, use elapsed instead)
        elapsed [3]: Total time elapsed within the current song in seconds, but with higher resolution.
        duration [5]: Duration of the current song in seconds.
        bitrate: instantaneous bitrate in kbps
        xfade: crossfade in seconds
        mixrampdb: mixramp threshold in dB
        mixrampdelay: mixrampdelay in seconds
        audio: The format emitted by the decoder plugin during playback, format: samplerate:bits:channels. See Global Audio Format for a detailed explanation.
        updating_db: job id
        error: if there is an error, returns message here
        """

        status = [
            ("volume", int(app.player.volume * 100)),
            ("repeat", int(self._options.repeat)),
            ("random", int(self._options.shuffle)),
            ("single", int(self._options.single)),
            ("consume", 0),
            ("playlist", self._pl_ver),
            ("playlistlength", int(bool(app.player.info))),
            ("mixrampdb", 0.0),
            ("state", state),
        ]

        if info:
            status.append(("audio", "%d:%d:%d" % (
                info("~#samplerate") or 0,
                info("~#bitdepth") or 0,
                info("~#channels") or 0)))
            total_time = int(info("~#length"))
            elapsed_time = int(app.player.get_position() / 1000)
            elapsed_exact = "%1.3f" % (app.player.get_position() / 1000.0)
            status.extend([
                ("song", 0),
                ("songid", self._get_id(info)),
            ])

            if state != "stop":
                status.extend([
                    ("time", "%d:%d" % (elapsed_time, total_time)),
                    ("elapsed", elapsed_exact),
                    ("bitrate", info("~#bitrate")),
                ])

        return status

    def currentsong(self):
        info = self._app.player.info
        if info is None:
            return None

        parts = []
        parts.append("file: %s" % info("~filename"))
        parts.append(format_tags(info))
        parts.append("Time: %d" % int(info("~#length")))
        parts.append("Pos: %d" % 0)
        parts.append("Id: %d" % self._get_id(info))

        return "\n".join(parts)

    def playlistinfo(self, start=None, end=None):
        if start is not None and start > 1:
            return None

        return self.currentsong()

    def playlistid(self, songid=None):
        return self.currentsong()

    def plchanges(self, version):
        if version != self._pl_ver:
            return self.currentsong()

    def plchangesposid(self, version):
        info = self._app.player.info
        if version != self._pl_ver and info:
            parts = []
            parts.append("file: %s" % info("~filename"))
            parts.append("Pos: %d" % 0)
            parts.append("Id: %d" % self._get_id(info))
            return "\n".join(parts)


class MPDServer(BaseTCPServer):

    def __init__(self, app, config, port):
        self._app = app
        self._config = config
        super(MPDServer, self).__init__(port, MPDConnection, const.DEBUG)

    def handle_init(self):
        print_d("Creating the MPD service")
        self.service = MPDService(self._app, self._config)

    def handle_idle(self):
        print_d("Destroying the MPD service")
        self.service.destroy()
        del self.service

    def log(self, msg):
        print_d(msg)


class MPDRequestError(Exception):

    def __init__(self, msg, code=AckError.UNKNOWN, index=None):
        self.msg = msg
        self.code = code
        self.index = index
        debug_print('MPDRequestError', msg=msg, code=code, index=index)

class Commands():
    command_list_begin = "command_list_begin"
    command_list_ok_begin = "command_list_ok_begin"
    command_list_end = "command_list_end"
    command_list_begin_set = {command_list_begin, command_list_ok_begin}

class MPDConnection(BaseTCPConnection):


    _commands = {}

    #  ------------ connection interface  ------------

    def handle_init(self, server):
        service = server.service
        self.service = service
        service.add_connection(self)

        str_version = ".".join(map(str, service.version))
        self._buf = bytearray(("OK MPD %s\n" % str_version).encode("utf-8"))
        self._read_buf = bytearray()

        # command list processing state
        self._command_list_started = False

        # if True, we should reply to all executed command
        self._command_list_ok = False
        
        # commands received so far
        self._command_list = []

        self.permission = self.service.default_permission

        self.start_write()
        self.start_read()

    def handle_read(self, data):
        self._feed_data(data)

        while True:
            line = self._get_next_line()
            if line is None:
                break

            self.log("-> " + repr(line))

            try:
                cmd_name, args = parse_command(line)
            except ParseError:
                # TODO: not sure what to do here re command lists
                print('PARSE ERROR')
                continue

            print(Colorise.green(f'-> {cmd_name} {args}'))
            
            try:
                self._handle_command(cmd_name, args)
            except MPDRequestError as e:
                self._error(e.msg, e.code, e.index, command=cmd_name)
                self._command_list_started = False
                self._command_list = []

    def handle_write(self):
        data = self._buf[:]
        del self._buf[:]
        return data

    def can_write(self):
        return bool(self._buf)

    def handle_close(self):
        self.log("connection closed")
        self.service.remove_connection(self)
        del self.service

    #  ------------ rest ------------

    def authenticate(self, password):
        # FIXME: store password as hash
        if password == self.service._config.config_get("password"):
            self.permission = Permissions.PERMISSION_ALL
        else:
            self.permission = self.service.default_permission
            raise MPDRequestError("Password incorrect", AckError.PASSWORD)

    def log(self, msg):
        if const.DEBUG:
            print_d("[%s] %s" % (self.name, msg))

    def _feed_data(self, new_data):
        """Feed new data into the read buffer"""

        self._read_buf.extend(new_data)

    def _get_next_line(self):
        """Returns the next line from the read buffer or None"""

        try:
            index = self._read_buf.index(b"\n")
        except ValueError:
            return None

        line = bytes(self._read_buf[:index])
        del self._read_buf[:index + 1]
        return line

    def write_line(self, line):
        """Writes a line to the client"""

        assert isinstance(line, str)
        self.log("<- " + repr(line))
        print(Colorise.blue(f'<- {line}'))

        self._buf.extend(line.encode("utf-8", errors="replace") + b"\n")

    def ok(self):
        self.write_line("OK")

    def list_ok(self):
        self.write_line("list_OK")

    def _error(self, msg=None, code=None, index=None, command=""):
        error = []
        error.append("ACK [%d" % code)
        if index is not None:
            error.append("@%d" % index)
        error.append("] {%s}" % command)
        if msg is not None:
            error.append(" %s" % msg)
        self.write_line("".join(error))
        debug_print('ERROR', msg=msg, code=code, index=index, command=command)

    def _handle_command(self, command, args):
        # end of command list: execute collected commands
        if command == Commands.command_list_end:
            if not self._command_list_started:
                self._error("command_list_end before begin", command=command)
                return

            for i, (cmd_name, args) in enumerate(self._command_list):
                try:
                    self._exec_command(cmd_name, args)
                except MPDRequestError as e:
                    # reraise with index
                    raise MPDRequestError(e.msg, e.code, i)

            self.ok()
            self._command_list_started = False
            self._command_list = []
            return

        # start of command list: wait for commands to execute in batch
        if command in Commands.command_list_begin_set:
            if self._command_list_started:
                raise MPDRequestError("begin without end")
            if self._command_list:
                print(f"Command list wasn't empty: {self._commands}")
                self._command_list = []

            self._command_list_started = True
            self._command_list_ok = command == Commands.command_list_ok_begin    
            return

        # add new command to batch
        if self._command_list_started:
            self._command_list.append((command, args))
        # execute command immediately
        else:
            self._exec_command(command, args)

    def _exec_command(self, command, args):
        if command not in self._commands:
            print(f'Unhandled command "{command}", fallback to `OK`')

            # Unhandled command, default to OK for now..
            if not self._command_list_started:
                self.ok()
            elif self._command_list_ok:
                self.list_ok()
            return

        cmd, do_ack, permission = self._commands[command]
        if permission != (self.permission & permission):
            raise MPDRequestError("Insufficient permission", AckError.PERMISSION)

        cmd(self, self.service, args)

        if self._command_list_started:
            if self._command_list_ok:
                self.list_ok()
        elif do_ack:
            self.ok()

    @classmethod
    def Command(cls, name, ack=True, permission=Permissions.PERMISSION_ADMIN):

        def wrap(func):
            assert name not in cls._commands, name
            cls._commands[name] = (func, ack, permission)
            return func

        return wrap

    @classmethod
    def list_commands(cls):
        """A list of supported commands"""

        return cls._commands.keys()


def _verify_length(args, length):
    if not len(args) >= length:
        raise MPDRequestError("Wrong arg count")


def _parse_int(arg):
    try:
        return int(arg)
    except ValueError:
        raise MPDRequestError("invalid arg")


def _parse_bool(arg):
    try:
        value = int(arg)
        if value not in (0, 1):
            raise ValueError
    except ValueError:
        raise MPDRequestError("invalid arg")
    else:
        return bool(value)


def _parse_range(arg):
    try:
        values = [int(v) for v in arg.split(":")]
    except ValueError:
        raise MPDRequestError("arg in range not a number")

    if len(values) == 1:
        return (values[0], values[0] + 1)
    elif len(values) == 2:
        return values
    else:
        raise MPDRequestError("invalid range")


@MPDConnection.Command("idle", ack=False)
def _cmd_idle(conn, service, args):
    service.register_idle(conn, args)


@MPDConnection.Command("ping", permission=Permissions.PERMISSION_NONE)
def _cmd_ping(conn, service, args):
    return


@MPDConnection.Command("password", permission=Permissions.PERMISSION_NONE)
def _cmd_password(conn, service, args):
    _verify_length(args, 1)
    conn.authenticate(args[0])


@MPDConnection.Command("noidle")
def _cmd_noidle(conn, service, args):
    service.unregister_idle(conn)


@MPDConnection.Command("close", ack=False,
        permission=Permissions.PERMISSION_NONE)
def _cmd_close(conn, service, args):
    conn.close()


@MPDConnection.Command("play")
def _cmd_play(conn, service, args):
    service.play()


@MPDConnection.Command("listplaylists")
def _cmd_listplaylists(conn, service, args):
    unimplemented('listplaylists')


@MPDConnection.Command("list")
def _cmd_list(conn, service, args):
    unimplemented('list')


@MPDConnection.Command("playid")
def _cmd_playid(conn, service, args):
    _verify_length(args, 1)
    songid = _parse_int(args[0])
    service.playid(songid)


@MPDConnection.Command("pause")
def _cmd_pause(conn, service, args):
    value = None
    if args:
        _verify_length(args, 1)
        value = _parse_bool(args[0])
    service.pause(value)


@MPDConnection.Command("stop")
def _cmd_stop(conn, service, args):
    service.stop()


@MPDConnection.Command("next")
def _cmd_next(conn, service, args):
    service.next()


@MPDConnection.Command("previous")
def _cmd_previous(conn, service, args):
    service.previous()


@MPDConnection.Command("repeat")
def _cmd_repeat(conn, service, args):
    _verify_length(args, 1)
    value = _parse_bool(args[0])
    service.repeat(value)


@MPDConnection.Command("random")
def _cmd_random(conn, service, args):
    _verify_length(args, 1)
    value = _parse_bool(args[0])
    service.random(value)


@MPDConnection.Command("single")
def _cmd_single(conn, service, args):
    _verify_length(args, 1)
    value = _parse_bool(args[0])
    service.single(value)


@MPDConnection.Command("setvol")
def _cmd_setvol(conn, service, args):
    _verify_length(args, 1)
    value = _parse_int(args[0])
    service.setvol(value)


@MPDConnection.Command("status")
def _cmd_status(conn, service, args):
    status = service.status()
    for k, v in status:
        conn.write_line("%s: %s" % (k, v))


@MPDConnection.Command("stats")
def _cmd_stats(conn, service, args):
    status = service.stats()
    for k, v in status:
        conn.write_line("%s: %s" % (k, v))


@MPDConnection.Command("currentsong")
def _cmd_currentsong(conn, service, args):
    stats = service.currentsong()
    if stats is not None:
        conn.write_line(stats)


@MPDConnection.Command("count")
def _cmd_count(conn, service, args):
    unimplemented('count')
    conn.write_line("songs: 0")
    conn.write_line("playtime: 0")


@MPDConnection.Command("plchanges")
def _cmd_plchanges(conn, service, args):
    _verify_length(args, 1)
    version = _parse_int(args[0])
    changes = service.plchanges(version)
    if changes is not None:
        conn.write_line(changes)


@MPDConnection.Command("plchangesposid")
def _cmd_plchangesposid(conn, service, args):
    _verify_length(args, 1)
    version = _parse_int(args[0])
    changes = service.plchangesposid(version)
    if changes is not None:
        conn.write_line(changes)


@MPDConnection.Command("listallinfo")
def _cmd_listallinfo(*args):
    _cmd_currentsong(*args)


@MPDConnection.Command("seek")
def _cmd_seek(conn, service, args):
    _verify_length(args, 2)
    songpos = _parse_int(args[0])
    time_ = _parse_int(args[1])
    service.seek(songpos, time_)


@MPDConnection.Command("seekid")
def _cmd_seekid(conn, service, args):
    _verify_length(args, 2)
    songid = _parse_int(args[0])
    time_ = _parse_int(args[1])
    service.seekid(songid, time_)


@MPDConnection.Command("seekcur")
def _cmd_seekcur(conn, service, args):
    _verify_length(args, 1)

    relative = False
    time_ = args[0]
    if time_.startswith(("+", "-")):
        relative = True

    try:
        time_ = int(time_)
    except ValueError:
        raise MPDRequestError("arg not a number")

    service.seekcur(time_, relative)


@MPDConnection.Command("outputs")
def _cmd_outputs(conn, service, args):
    conn.write_line("outputid: 0")
    conn.write_line("outputname: dummy")
    conn.write_line("outputenabled: 1")


@MPDConnection.Command("commands", permission=Permissions.PERMISSION_NONE)
def _cmd_commands(conn, service, args):
    for name in conn.list_commands():
        conn.write_line("command: " + str(name))


@MPDConnection.Command("tagtypes")
def _cmd_tagtypes(conn, service, args):
    for mpd_key, _ql_key in TAG_MAPPING:
        conn.write_line(mpd_key)


@MPDConnection.Command("lsinfo")
def _cmd_lsinfo(conn, service, args):
    _verify_length(args, 1)


@MPDConnection.Command("playlistinfo")
def _cmd_playlistinfo(conn, service, args):
    if args:
        _verify_length(args, 1)
        start, end = _parse_range(args[0])
        result = service.playlistinfo(start, end)
    else:
        result = service.playlistinfo()
    if result is not None:
        conn.write_line(result)


@MPDConnection.Command("playlistid")
def _cmd_playlistid(conn, service, args):
    if args:
        songid = _parse_int(args[0])
    else:
        songid = None
    result = service.playlistid(songid)
    if result is not None:
        conn.write_line(result)
