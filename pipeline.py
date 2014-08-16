# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import os.path
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable

import warc #for manipulating warc files (re-open, sample video, re-close)

# check the seesaw version
if StrictVersion(seesaw.__version__) < StrictVersion("0.1.5"):
    raise Exception("This pipeline needs seesaw version 0.1.5 or higher.")


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_LUA will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
WGET_LUA = find_executable(
    "Wget+Lua",
    ["GNU Wget 1.14.lua.20130523-9a5c"],
    [
        "./wget-lua",
        "./wget-lua-warrior",
        "./wget-lua-local",
        "../wget-lua",
        "../../wget-lua",
        "/home/warrior/wget-lua",
        "/usr/bin/wget-lua"
    ]
)

if not WGET_LUA:
    raise Exception("No usable Wget+Lua found.")

###########################################################################
# Determine if FFMPEG is available
# Should probably utilize an ffmpeg build (or source) distributed from the
# repo to avoid nasty API incompatibilities between FFMPEG versions.
FFMPEG = find_executable(
    "ffmpeg",
    ["ffmpeg version 2"],
    [
        "/usr/bin/ffmpeg",
        "/usr/local/bin/ffmpeg",
        "./ffmpeg"
    ]
)

if not FFMPEG:
    raise Exception("No usable ffmpeg found.")

exit();

###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20140809.03"
USER_AGENT = 'ArchiveTeam'
TRACKER_ID = 'twitchtv'
TRACKER_HOST = 'tracker.archiveteam.org'


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "CheckIP")
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            result = socket.gethostbyname('twitch.tv')

            if not result.startswith('192.16.71.'):
                item.log_output('Got IP address: {0}'.format(result))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, "PrepareDirectories")
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item["item_name"]
        escaped_item_name = hashlib.sha1(item_name).hexdigest()
        dirname = "/".join((item["data_dir"], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item["item_dir"] = dirname
        item["warc_file_base"] = "%s-%s-%s" % (self.warc_prefix, escaped_item_name,
            time.strftime("%Y%m%d-%H%M%S"))

        open("%(item_dir)s/%(warc_file_base)s.warc.gz" % item, "w").close()


# Will utilize ffmpeg to sample the downloaded item.
#
# First, sample the video at its native resolution.  This sampling ought to be
# regular. That is, we should sample the same frame in every period. Ex.) for a
# 30 fps video, we should always grab the Nth frame at each second.
#
# This sampling rate should scale with the length of the video. A short video
# might be afforded 2 frames per second, while an extra long video might only
# be afforded 1 frame every 2 or 3 seconds.
#
# Second, after taking a native-resolution snapshot of the video, 
#
# 1.) shrink it down to a small but visible resolution.
# 2.) cut the framerate down to a low but still motion-preserving number.
#     (frame-dropping)
#
# Both of these parameters ought to scale with the length of the
# source video. A relatively short video might be able to get away with
# 480p resolution, but a longer one should be cut down to 360p or even
# 240p resolution. A short video might have a higher preserved framerate,
# but not a longer video.
#
# This high-fidelity data from taking native-resolution snapshots, in
# combination with low-fidelity data from shrinking the resolution and dropping
# frames, will (hopefully) constitute a minimum viable dataset that might be of
# use to someone in the future.
class Sample(SimpleTask):
    def __init__(self):
        SimpleTask__init__(self, "Sample")

    def process(self, item):

    # assert that this item is flagged for sampling. If not,
    # return immediately. We don't want to butcher uploads that
    # have been determined to be worth saving in their original
    # state.
    #
    # Presumably, the tracker is tagging these items as something
    # appropriate. Alternately, one could create a "Phase 3" grab
    # and know for a fact that we are only receiving videos that
    # should be sampled. In which case, one may skip the item_type
    # check and proceed directly to sampling.

    item_name = item['item_name']
    item_type, item_value = item_name.split(':', 1)

    item['item_type'] = item_type
    item['item_value'] = item_value

    assert item_type in ('video-bulk', 'url-bulk')

    # unpack warc, call samplers, repack warc
    if item_type == 'video-bulk' || 'url-bulk':

        # At this point, wget has already dropped a .warc.gz file. Need to
        # unpack it.

        # Oh dear. It's looking like manipulating the warc file would
        # require more careful deliberation than just "sample the data
        # and put it back into the warc." Lots of metadata.
        #
        # http://warc.readthedocs.org/en/latest/
        #
        # It looks like "WARC-Truncated" is the named field we would want
        # according to the WARC spec:
        #
        # http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf
        #
        # Could rename whichever record in the .warc.gz holds the flv
        # (or other video file)
        # 
        # Planned method of attack:
        #
        # 0.) Unpack the .warc.gz
        # 1.) Extract the video payload of the original record.
        # 2.) Sample the video payload with native-resolution snapshots.
        # (SnapShot() helper) 
        # 3.) Shrink the video payload. (ShrinkRay() helper)
        # 4.) Create new record with "conversion" profile, 
        # referencing the original record with WARC-Refers-To header. Store
        # this snapshot material in the new record (in a tarball or
        # something? Would a metadata record be useful to describe how to
        # handle the tarball? (Lots of different ways to display slideshows,
        # which is basically what this is.))
        # 5.) Replace original record payload with shrunken payload, mark
        # the original record as WARC-Truncated.
        # 6.) Repack the .warc.gz

        # NOTE TO SELF: the payloads of record objects INCLUDE the HTTP
        # response headers before the actual content response.
        # NOTE TO SELF: Can extract uncompressed payloads directly from
        # compressed warcs (.warc.gz files) ; hoping that writing records
        # is similarly capable.

        # should probably also report compression statistics to the tracker

    # Item type is not marked as "video-bulk" from tracker.
    # Carry on. Nothing to do here.
    else
        return;

    ###################
    # Sampling routines
    #

    # High fidelity snapshots
    def SnapShot():

        # figure out length of video and develop native-resolution frame
        # sampling rate based off of this length.

        # begin to sample the video with ffmpeg using options determined
        # from the length of video
        call(["ffmpeg", ""])

        # assert that ffmpeg exited successfully


    # Low fidelity, shrinked video
    def ShrinkRay():

        # figure out length of video and develop number of frames to
        # drop out of every FPS interval.

        # begin to shrink the video with ffmpeg using options determined
        # from length of video
        call(["ffmpeg", ""])

        # assert that ffmpeg exited successfully

    #end of Sample()

class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        # NEW for 2014! Check if wget was compiled with zlib support
        # Shouldn't that happen _before_ the download is finished?
        if os.path.exists("%(item_dir)s/%(warc_file_base)s.warc"):
            raise Exception('Please compile wget with zlib support!')

        os.rename("%(item_dir)s/%(warc_file_base)s.warc.gz" % item,
              "%(data_dir)s/%(warc_file_base)s.warc.gz" % item)

        shutil.rmtree("%(item_dir)s" % item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()


CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'twitchtv.lua'))


def stats_id_function(item):
    # NEW for 2014! Some accountability hashes and stats.
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class WgetArgs(object):
    def realize(self, item):
        wget_args = [
            WGET_LUA,
            "-U", USER_AGENT,
            "-nv",
            "--lua-script", "twitchtv.lua",
            "-o", ItemInterpolation("%(item_dir)s/wget.log"),
            "--no-check-certificate",
            "--output-document", ItemInterpolation("%(item_dir)s/wget.tmp"),
            "--truncate-output",
            "-e", "robots=off",
            "--no-cookies",
            "--rotate-dns",
            # "--recursive", "--level=inf",
            "--page-requisites",
            "--timeout", "60",
            "--tries", "inf",
            "--span-hosts",
            "--waitretry", "3600",
#             "--domains", "twitch.tv,justin.tv,jtvnw.net",
            "--warc-file",
                ItemInterpolation("%(item_dir)s/%(warc_file_base)s"),
            "--warc-header", "operator: Archive Team",
            "--warc-header", "twitchtv-dld-script-version: " + VERSION,
            "--warc-header", ItemInterpolation("twitchtv-user: %(item_name)s"),
        ]

        # Randomly get the assets
        if random.randint(1, 10) == 1:
            wget_args.extend(["--domains", "twitch.tv,justin.tv,jtvnw.net", ])
        else:
            wget_args.extend(["--domains", "twitch.tv,justin.tv", ])

        item_name = item['item_name']
        item_type, item_value = item_name.split(':', 1)

        item['item_type'] = item_type
        item['item_value'] = item_value

        assert item_type in ('video', 'url')

        if item_type == 'video':
            video_id, username = item_value.split(':', 1)
            video_type = video_id[0:1]
            video_num = video_id[1:]

            assert video_type in ('a', 'b', 'c')

            # I guess we should have scraped for a video type since they don't
            # match the video ID.
            for video_type_ in ('a', 'b', 'c'):
                wget_args.append('http://www.twitch.tv/{0}/{1}/{2}'.format(username, video_type_, video_num))

            wget_args.append('https://api.twitch.tv/kraken/videos/{0}'.format(video_id))

        elif item_type == 'url':
            # This should be a URL to a flv
            wget_args.append(item_value)

        else:
            raise Exception('Unknown item')

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)


###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title="Twitch",
    project_html="""
        <img class="project-logo" alt="Project logo" src="http://archiveteam.org/images/d/d4/Twitch_Logo.png" height="50px" title="PRAISE HELIX!"/>
        <h2>Twitch Phase 2: Content Grab. <span class="links"><a href="http://twitch.tv/">Website</a> &middot; <a href="http://tracker.archiveteam.org/twitchtv/">Leaderboard</a></span></h2>
        <p>Please use low concurrency when working on this project. <em style="font-weight: bold;"><a href="https://archive.org/donate/" title="Do it for Kenya.">Donate to IA for disk space!</a></em></p>
    """,
    utc_deadline=datetime.datetime(2014, 8, 27, 23, 59, 0)
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker("http://%s/%s" % (TRACKER_HOST, TRACKER_ID), downloader,
        VERSION),
    PrepareDirectories(warc_prefix="twitchtv"),
    WgetDownload(
        WgetArgs(),
        max_tries=2,
        accept_on_exit_code=[0, 8],
        env={
            "item_dir": ItemValue("item_dir"),
            "item_value": ItemValue("item_value"),
            "item_type": ItemValue("item_type"),
        }
    ),
    PrepareStatsForTracker(
        defaults={"downloader": downloader, "version": VERSION},
        file_groups={
            "data": [
                ItemInterpolation("%(item_dir)s/%(warc_file_base)s.warc.gz")
            ]
        },
        id_function=stats_id_function,
    ),
    Sample(),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=4, default="1",
        name="shared:rsync_threads", title="Rsync threads",
        description="The maximum number of concurrent uploads."),
        UploadWithTracker(
            "http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation("%(data_dir)s/%(warc_file_base)s.warc.gz")
            ],
            rsync_target_source_path=ItemInterpolation("%(data_dir)s/"),
            rsync_extra_args=[
                "--recursive",
                "--partial",
                "--partial-dir", ".rsync-tmp",
                "--sockopts=SO_SNDBUF=16777216,SO_RCVBUF=16777216"  # speedbooster!!!
            ]
            ),
    ),
    SendDoneToTracker(
        tracker_url="http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue("stats")
    )
)
