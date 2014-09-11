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

# for properly parsing command line strings for insertion into call()s
import shlex
# for globbing files in a given path
import glob
# for making subprocesses (ffmpeg, tar, etc)
from subprocess import call
# for file length
import os
# for in-memory binary strings (rather than on-file)
from cStringIO import StringIO
# for manipulating warc files (open, write, read, close, compress)
import warc

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable

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
# However, if the options used are relatively simple, using distro-provided
# ffmpeg builds shouldn't be too problematic. Just be sure to add a metadata
# WARCRecord indicating the version string of the ffmpeg that is used.
FFMPEG = find_executable(
    "ffmpeg",
    ["ffmpeg version 2"],
    [
        "/usr/bin/ffmpeg",
        "/usr/local/bin/ffmpeg",
        "./ffmpeg"
    ],
    "-version"
)

if not FFMPEG:
    raise Exception("No usable ffmpeg found.")

###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20140827.01"
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
        SimpleTask.__init__(self, "Sample")

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

        # Item type is not marked as "video-bulk" from tracker.
        # Carry on. Nothing to do here.
        if item_type != 'video-bulk' or 'url-bulk':
            return

        # ok. This is an item that needs to be sampled.

        # remember where we started from so we can get back there and
        # not mess up the expectations for the rest of stages in the
        # pipeline
        original_path = os.getcwd()

        # get to item_dir ; begin work
        os.chdir(item['item_dir'])

        # we will need some data from the warcfile
        warcinfo_record_ID = ""
        metadata_record_ID = ""
        truncated_record_ID = ""

        # set up old and new warc files for reading and writing, respectively.
        # If a file ends in *.gz for writing, the warc library handles gz
        # compression transparently.
        old_warc_file = warc.open("%(warc_file_base)s.warc.gz" % item)
        new_warc_file = warc.open("%(warc_file_base)s-POSTPROCESSED.warc.gz" % item , "w")

        # ------------------------ Start of main for loop -------------------#

        # and here... we... go
        for record in old_warc_file:

            # Firstly, we detect whether the record we're iterating over holds
            # data we'll need later. If so, behave appropriately. After the
            # if-elif-elif dance, we proceed to copy each record into a new
            # record in the %(warc_file_base)s-POSTPROCESSED.warc.gz file,
            # modifying as necesary (truncated long records, etc)

            # ------------------------ Check for data -------------------------#

            # Grab the lengthy payload (the flv file); if the content-length is
            # longer than ~5MiB, and the record is of the "response" type, then
            # this record *probably* has the flv file.
            if ((long(record['Content-Length']) >= 5000000) and record['WARC-Type'] == "response"):

                # need the record id of the original flv record. Will refernece
                # it in truncated record.
                truncated_record_id = record['warc-record-id']

                # add "WARC-Truncated" to this record, indicating that it has
                # been truncated due to length.
                record['warc-truncated'] = "length"

                # extract the payload
                tempfile = open("intermediate.int", 'wb')
                for line in record.payload:
                    tempfile.write(line)
                tempfile.close()

                # put the payload back; iterating through record.payload
                # invokes a generator on the payload that seems to
                # "eat up" the payload in the original file. I say so because
                # attempting to, say, write the payload out twice (to TWO files)
                # will fail, as will any attempt to read out the payload again
                # without first "putting it back." (I'd love an explanation for
                # just what's going on here; but for now, this hack works)
                # (for the record with the long content-length, we end up reading
                # the payload twice; once here, to get it to a separate file, and
                # once again, in COPY PAYLOAD, to write out a truncated version to
                # the new warc file)
                stream = StringIO(open("intermediate.dat", 'rb').read())
                stream.seek(0, os.SEEK_END)
                streamlength = stream.tell()
                stream.seek(0)
                record.payload = warc.utils.FilePart(fileobj = stream, length = streamlength)

                # can't close the stream yet for some reason. This might
                # introduce leaks of some sort, so keep an eye on it.
                # The relevant error: "IO Operation on a closed file."
                # I suspect this operation occurs somewhere in the warc library,
                # and i'm hoping that the stream object just falls out of scope
                # at some point other than when the entire pipeline shuts down.
                # stream.close()

            # Adjust the warcinfo record to note that we also utilized ffmpeg
            elif (record['WARC-Type'] == "warcinfo"):

                # grab the record-id for later use in resource records
                warcinfo_record_ID = record['warc-record-id']

                # gotta add another "software" key to the content-block of the
                # warcinfo record that indicates the use of ffmpeg.
                warcinfo_stream = StringIO()
                for line in record.payload:
                    warcinfo_stream.write(line)

                # trailing \r\n\r\n is already present in the payload; just seek back
                # two bytes (yes, the second \r\n will get clobbered; potential unicode
                # byte-length issues here) and then tack on the additional lines you
                # need to like so:
                warcinfo_stream.seek(-2,os.SEEK_END)
                warcinfo_stream.write("software: ffmpeg/2.3.1\r\n\r\n")
                warcinfo_stream.seek(0, os.SEEK_END)
                warcinfo_stream_len = warcinfo_stream.tell()
                warcinfo_stream.seek(0)
                record.payload = warc.utils.FilePart(fileobj=warcinfo_stream,length=warcinfo_stream_len)

            # Get the metadata record's warc-record-id for later resource
            # records.
            elif (record['WARC-Type'] == "metadata"):

                metadata_record_ID = record['warc-record-id']

            # End of conditionals. Proceed to write the new record to the
            # post-processed warcfile.

            # ------------------------ Copy Record -------------------------#

            # COPY HEADER

            # Should we add defaults=False ? It seems that some additional headers
            # are added in WARCHeader as well as WARCRecord. However, they don't
            # seem harmful: digests and timestamps.
            new_header = warc.WARCHeader(record.header)

            # COPY PAYLOAD

            # if the current record gets truncated, then set the content-length
            # to the new, truncated length as per spec.
            truncated_flag = None

            # SHORT record payloads
            if long(record['content-length']) < 500000:

                #print "Copying payload..."
                new_payload = StringIO()
                for line in record.payload:
                    new_payload.write(line)
                #if we don't seek back to 0, new_payload.read() is empty
                new_payload.seek(0)
                #print "Done copying payload."

            # LONG record payloads (the one that probably has video data)
            else:

            #print "Found long content-length. Truncating..."
                new_payload = StringIO()
                decrement = 25
                #Grab some lines
                #print "Gonna grab some lines. Decrement: ", decrement
                for line in record.payload:
                    #print "Grabbing a line."
                    new_payload.write(line)
                    decrement -= 1
                    #print "Decrement: ", decrement
                    if decrement == 0:
                        break
                # be kind: rewind
                new_payload.seek(0)
                truncated_flag = True

                #print "Done truncating."

            # CREATE RECORD FROM HEADER AND PAYLOAD

            new_rec = warc.WARCRecord(payload=new_payload.read(), headers=new_header, defaults=False)

            # if this record happened to be one that got truncated, then we
            # need to adjust its content-length header.
            if truncated_flag:

                #print "Adjusting content-length header"

                # From page 9 of the ISO WARC Standard:
                #
                # "The WARC-Truncated field may be used on any WARC record. The WARC
                # field Content-Length shall still report the actual truncated size of
                # the record block."

                # Get the length of the truncated content-block and set
                # Content-Length header appropriately
                new_payload.seek(0)
                new_payload.seek(0, os.SEEK_END)
                thelength = new_payload.tell()
                new_rec['content-length'] = str(thelength)
                new_payload.seek(0)

            # WRITE THE NEW RECORD OUT TO THE NEW WARCFILE

            # (the warc library handles the gz-compression and putting each record
            # in a separate gz "member" transparently; no need to much with the gzip
            # library ourselves)

            #print "Copying record to new .warc.gz"
            new_warc_file.write_record(new_rec)
            #print "Done copying record to new .warc.gz"
            #print "\n\n"

        #------------------------ END OF MAIN FOR LOOP ------------------------#

        # at this point, we have a new warcfile with copied and truncated
        # records; now, we need to sample the content and add these "conversion"
        # records to the warc file.

        # Should probably delete old warc at this point, since new warcfile has all
        # of the old records, and we've already got another copy of the main
        # payload. If we proceed to write out the full newfile with the shrunken
        # payload before deleting the old warc, we'll basically be using nearly
        # 3x the interim diskspace rather than 2x. (Don't get me wrong, I'd love
        # to have more of a generator-like setup that negates the need to use
        # twice the disk space, but it's beyond the scope of my abilities at the
        # moment and I don't think I'd be able to get up to speed before the
        # deadline for this project drops (August 27 2014) Update: LOL Twitch is
        # already deleting things on August 26; oh well, I suppose this code
        # could come in handy if the IA suddenly needs to compress lots of
        # material)

        # Now, we need to convert the flv, and add conversion records

        # Our "payload.flv" is not quite an flv yet; the payload still includes the
        # HTTP Response headers. We need to grep for "CRLFCRLF" and then chop off
        # anything prior to it, including it, leaving nothing but the flv file for
        # ffmpeg to work with.
        thefile = open("intermediate.int").read() # NOT A FILE; just a "str"
        theflv = thefile.split('\r\n\r\n')[1]
        writetheflv = open("samplethis.flv", "w")
        writetheflv.write(theflv)
        writetheflv.close()

        # Get Snapshots
        SnapShot()

        # Get shrinked video
        ShrinkRay()

        # Clean up
        print("********************* \n\n Removing temporary files; cleaning up \n\n*********************")
        # remove original file intermediates: "intermediate.int" and "samplethis.flv"
        rmargs = shlex.split("rm intermediate.int samplethis.flv")
        call(rmargs)

        # And we're done!
        new_warc_file.close()
        os.chdir(original_path)

    ###################
    # Sampling routines
    #

    # High fidelity snapshots
    def SnapShot():

        # TODO:
        # figure out length of video and develop native-resolution frame
        # sampling rate based off of this length.

        print("********************* \n\n Getting snapshots. \n\n*********************")

        os.environ["FFREPORT"] = "file=ffmpeg-snapshots.log"

        # snapshot
        # This is the "proper" way to handle complex command lines with lots of args
        # https://stackoverflow.com/questions/8581140/python-subprocess-call-with-arguments-having-multiple-quotations
        ffmpegsnapshotargs = shlex.split("ffmpeg -i samplethis.flv -vf fps=fps=1/15 -f image2 -q:v 1 images%05d.jpg")
        call(ffmpegsnapshotargs)

        print("********************* \n\n Compressing snapshots. \n\n*********************")

        imagelist = glob.glob("*.jpg")
        imageliststring = ' '.join(imagelist)
        tarcommand = "tar -czvf snapshots.tar.gz " + imageliststring

        # compress all the snapshots
        tarargs = shlex.split(tarcommand)
        call(tarargs)

        # delete jpgs
        rmcommand = "rm " + imageliststring
        rmargs = shlex.split(rmcommand)
        call(rmargs)

        os.environ["FFREPORT"] = ""

        # Add ffmpeg log record
        ffmpegsampleheader = warc.WARCHeader({
            "WARC-Type": "resource",
            "WARC-Warcinfo-ID": warcinfo_record_ID,
            "Content-Type": "text/plain",
            "WARC-Concurrent-To": metadata_record_ID
        })
        ffmpegsamplepayload = StringIO(open("ffmpeg-snapshots.log").read()).getvalue()
        ffmpegsamplerecord = warc.WARCRecord(headers=ffmpegsampleheader,payload=ffmpegsamplepayload)
        new_warc_file.write_record(ffmpegsamplerecord)

        # Add the actual snapshot record
        snapshotrecord = warc.WARCRecord(
            headers = warc.WARCHeader({
                "WARC-Type": "conversion",
                "Content-Type": "application/x-gtar",
                "WARC-Refers-To": truncated_record_ID
            }),
            payload = StringIO(open("snapshots.tar.gz").read()).getvalue()
        )
        new_warc_file.write_record(snapshotrecord)

        # remove snapshots and log
        call(shlex.split("rm snapshots.tar.gz ffmpeg-snapshots.log"))

        # end of SnapShot()

    # Low fidelity, shrinked video
    def ShrinkRay():

        # TODO:
        # figure out length of video and develop number of frames to
        # drop out of every FPS interval.

        print("********************* \n\n Shrinking Video. (This will take a while) \n\n*********************")

        os.environ["FFREPORT"] = "file=ffmpeg-shrinking.log"

        # shrink; using the webm format at this resolution cuts the file size by
        # *about* an order of magnitude, while still maintaining more-or-less
        # perfectly crisp detail and motion. I'm thinking we don't need to drop
        # frames, and that cutting the resolution down to this ~240P-level
        # resolution is good enough.

        # We really need to check for resolution and select an output resolution
        # appropriately; this one-liner only works for 16:9 inputs

        ffmpegshrinkargs = shlex.split("ffmpeg -i samplethis.flv -c:v libvpx -b:v 500K -c:a libvorbis -s 432x243 shrunken-to-webm.webm")
        call(ffmpegshrinkargs)

        # The final size of snapshots and shrunken video is anywhere from a fifth to
        # a seventh of the original file size.

        os.environ["FFREPORT"] = ""

        # add ffmpeg log record
        ffmpegshrinkheader = warc.WARCHeader({
            "WARC-Type": "resource",
            "WARC-Warcinfo-ID": warcinfo_record_ID,
            "Content-Type": "text/plain",
            "WARC-Concurrent-To": metadata_record_ID
        })
        ffmpegshrinkpayload = StringIO(open("ffmpeg-shrinking.log").read()).getvalue()
        ffmpegshrinkrecord = warc.WARCRecord(headers=ffmpegshrinkheader,payload=ffmpegshrinkpayload)
        new_warc_file.write_record(ffmpegshrinkrecord)

        # add actual shrunken webm record
        shrinkrecord = warc.WARCRecord(
            headers = warc.WARCHeader({
                "WARC-Type": "conversion",
                "Content-Type": "video/webm",
                "WARC-Refers-To": truncated_record_ID
            }),
            payload = StringIO(open("shrunken-to-webm.webm").read()).getvalue()
        )
        new_warc_file.write_record(shrinkrecord)

        # remove log file
        call(shlex.split("rm snapshots.tar.gz ffmpeg-shrinking.log"))

        # end of ShrinkRay()

    #end of class Sample(SimpleTask)

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
