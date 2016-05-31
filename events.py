#!/usr/bin/python
# Copyright (C) 2016 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>
#

from dateutil import parser

from datetime import datetime

import pandas

class Events(object):
    """ Class that 'eventizes' information for a given dataset.

    This class aims at providing with some granularity all of the events
    of a pre-formatted item. This is expected to help in later steps
    during the visualization platform.
    """

class Bugzilla(Events):
    """ Class used to 'eventize' Bugzilla items

    This splits information of each item based on a pre-configured mapping.
    There are several levels of events. These levels were created as a way
    to have more or less time consuming generation of events.
    """

    EVENT_OPEN = "ISSUE_OPEN"

    # Fields supported by this module (when a DataFrame is returned)
    ISSUE_ID = "id"
    ISSUE_EVENT = "eventtype"
    ISSUE_DATE = "date"
    ISSUE_OWNER = "owner"

    def __init__(self, items):
        """ Main constructor of the class

        :param items: original list of JSON that contains all info about a bug
        :type items: list
        """

        self.items = items


    def eventize(self, granularity):
        """ This splits the JSON information found at self.events into the
        several events. For this there are three different levels of time
        consuming actions: 1-soft, 2-medium and 3-hard.

        Level 1 provides events about open and closed issues.
        Level 2 provides events about the rest of the status updates.
        Level 3 provides events about the rest of the values in any of the
        fields.

        :param granularity: Levels of time consuming actions to calculate events
        :type granularity: integer

        :returns: Pandas dataframe with splitted events.
        :rtype: pandas.DataFrame
        """


        issue = {}
        issue[Bugzilla.ISSUE_ID] = []
        issue[Bugzilla.ISSUE_EVENT] = []
        issue[Bugzilla.ISSUE_DATE] = []
        issue[Bugzilla.ISSUE_OWNER] = []

        events = pandas.DataFrame()

        for item in self.items:
            bug_data = item["data"]
            if granularity == 1:
                # Open Date: filling a new event
                issue[Bugzilla.ISSUE_ID].append(bug_data['bug_id'][0]['__text__'])
                issue[Bugzilla.ISSUE_EVENT].append(Bugzilla.EVENT_OPEN)
                issue[Bugzilla.ISSUE_DATE].append(parser.parse(bug_data['creation_ts'][0]['__text__']))
                issue[Bugzilla.ISSUE_OWNER].append(bug_data['reporter'][0]["__text__"])

                # Adding the rest of the status updates (if there were any)
                if 'activity' in bug_data.keys():
                    activity = bug_data["activity"]
                    for change in activity:
                        if change["What"] == "Status":
                            # Filling a new event
                            issue[Bugzilla.ISSUE_ID].append(bug_data['bug_id'][0]['__text__'])
                            issue[Bugzilla.ISSUE_EVENT].append("ISSUE_" + change["Added"])
                            issue[Bugzilla.ISSUE_DATE].append(parser.parse(change["When"]))
                            issue[Bugzilla.ISSUE_OWNER].append(change["Who"])

            if granularity == 2:
                #TBD
                pass

            if granularity == 3:
                #TDB
                pass

        # Done in this way to have an order (and not a direct cast)
        events[Bugzilla.ISSUE_ID] = issue[Bugzilla.ISSUE_ID]
        events[Bugzilla.ISSUE_EVENT] = issue[Bugzilla.ISSUE_EVENT]
        events[Bugzilla.ISSUE_DATE] = issue[Bugzilla.ISSUE_DATE]
        events[Bugzilla.ISSUE_OWNER] = issue[Bugzilla.ISSUE_OWNER]

        return events


class Git(Events):
    """ Class used to 'eventize' Git items

    This splits information of each item based on a pre-configured mapping.
    There are several levels of events. These levels were created as a way
    to have more or less time consuming generation of events.
    """

    EVENT_COMMIT = "COMMIT"
    EVENT_FILE = "FILE_"

    # Fields supported by this module (when a DataFrame is returned)
    COMMIT_ID = "id"
    COMMIT_EVENT = "eventtype"
    COMMIT_DATE = "date"
    COMMIT_OWNER = "owner"

    FILE_EVENT = "fileaction"
    FILE_PATH = "filepath"
    FILE_ADDED_LINES = "addedlines"
    FILE_REMOVED_LINES = "removedlines"

    def __init__(self, items):
        """ Main constructor of the class

        :param items: original list of JSON that contains all info about a commit
        :type items: list
        """

        self.items = items


    def eventize(self, granularity):
        """ This splits the JSON information found at self.events into the
        several events. For this there are three different levels of time
        consuming actions: 1-soft, 2-medium and 3-hard.

        Level 1 provides events about commits
        Level 2 provides events about files
        Level 3 provides other events (not used so far)

        :param granularity: Levels of time consuming actions to calculate events
        :type granularity: integer

        :returns: Pandas dataframe with splitted events.
        :rtype: pandas.DataFrame
        """


        commit = {}
        # First level granularity
        commit[Git.COMMIT_ID] = []
        commit[Git.COMMIT_EVENT] = []
        commit[Git.COMMIT_DATE] = []
        commit[Git.COMMIT_OWNER] = []

        # Second level of granularity
        commit[Git.FILE_EVENT] = []
        commit[Git.FILE_PATH] = []
        commit[Git.FILE_ADDED_LINES] = []
        commit[Git.FILE_REMOVED_LINES] = []

        events = pandas.DataFrame()

        for item in self.items:
            commit_data = item["data"]
            if granularity == 1:
                commit[Git.COMMIT_ID].append(commit_data['commit'])
                commit[Git.COMMIT_EVENT].append(Git.EVENT_COMMIT)
                commit[Git.COMMIT_DATE].append(parser.parse(commit_data['AuthorDate']))
                commit[Git.COMMIT_OWNER].append(commit_data['Author'])

            #TODO: this will fail if no files are found in a commit (eg: merge)
            if granularity == 2:
                # Add extra info about files actions, if there were any
                if commit_data.has_key("files"):
                    files = commit_data["files"]
                    for f in files:
                        commit[Git.COMMIT_ID].append(commit_data['commit'])
                        commit[Git.COMMIT_EVENT].append(Git.EVENT_COMMIT)
                        commit[Git.COMMIT_DATE].append(parser.parse(commit_data['AuthorDate']))
                        commit[Git.COMMIT_OWNER].append(commit_data['Author'])
                        commit[Git.FILE_EVENT] = Git.EVENT_FILE + f["action"]
                        commit[Git.FILE_PATH] = f["file"]
                        if f["added"] == "-":
                            commit[Git.FILE_ADDED_LINES].append(0)
                        else:
                            commit[Git.FILE_ADDED_LINES].append(int(f["added"]))
                        if f["removed"] == "-":
                            commit[Git.FILE_REMOVED_LINES].append(0)
                        else:
                            commit[Git.FILE_REMOVED_LINES].append(int(f["removed"]))

            if granularity == 3:
                #TDB
                pass

        # Done in this way to have an order (and not a direct cast)
        events[Git.COMMIT_ID] = commit[Git.COMMIT_ID]
        events[Git.COMMIT_EVENT] = commit[Git.COMMIT_EVENT]
        events[Git.COMMIT_DATE] = commit[Git.COMMIT_DATE]
        events[Git.COMMIT_OWNER] = commit[Git.COMMIT_OWNER]
        if granularity == 2:
            events[Git.FILE_EVENT] = commit[Git.FILE_EVENT]
            events[Git.FILE_PATH] = commit[Git.FILE_PATH]
            events[Git.FILE_ADDED_LINES] = commit[Git.FILE_ADDED_LINES]
            events[Git.FILE_REMOVED_LINES] = commit[Git.FILE_REMOVED_LINES]

        return events



class Gerrit(Events):
    """ Class used to 'eventize' Gerrit items

    This splits information of each item based on a pre-configured mapping.
    There are several levels of events. These levels were created as a way
    to have more or less time consuming generation of events.
    """

    EVENT_OPEN = "CHANGESET_SENT"
    EVENT_ = "CHANGESET_"

    # Fields supported by this module (when a DataFrame is returned)
    CHANGESET_ID = "id"
    CHANGESET_EVENT = "eventtype"
    CHANGESET_DATE = "date"
    CHANGESET_OWNER = "owner"

    def __init__(self, items):
        """ Main constructor of the class

        :param items: original list of JSON that contains all info about a commit
        :type items: list
        """

        self.items = items

    def eventize(self, granularity):
        """ This splits the JSON information found at self.events into the
        several events. For this there are three different levels of time
        consuming actions: 1-soft, 2-medium and 3-hard.

        Level 1 provides events about commits
        Level 2 provides events about files
        Level 3 provides other events (not used so far)

        :param granularity: Levels of time consuming actions to calculate events
        :type granularity: integer

        :returns: Pandas dataframe with splitted events.
        :rtype: pandas.DataFrame
        """

        changeset = {}
        # First level granularity
        changeset[Gerrit.CHANGESET_ID] = []
        changeset[Gerrit.CHANGESET_EVENT] = []
        changeset[Gerrit.CHANGESET_DATE] = []
        changeset[Gerrit.CHANGESET_OWNER] = []

        events = pandas.DataFrame()

        for item in self.items:
            changeset_data = item["data"]
            if granularity == 1:
                # Changeset submission date: filling a new event
                changeset[Gerrit.CHANGESET_ID].append(changeset_data["number"])
                changeset[Gerrit.CHANGESET_EVENT].append(Gerrit.EVENT_OPEN)
                changeset[Gerrit.CHANGESET_DATE].append(datetime.fromtimestamp(int(changeset_data["createdOn"])))
                changeset[Gerrit.CHANGESET_OWNER].append(changeset_data["owner"]["username"])

                # Adding the closing status updates (if there was any)
                if changeset_data["status"] == 'ABANDONED' or \
                   changeset_data["status"] == 'MERGED':
                       closing_date = datetime.fromtimestamp(int(changeset_data["lastUpdated"]))
                       changeset[Gerrit.CHANGESET_ID].append(changeset_data["number"])
                       changeset[Gerrit.CHANGESET_EVENT].append(Gerrit.EVENT_ + changeset_data["status"])
                       changeset[Gerrit.CHANGESET_DATE].append(closing_date)
                       changeset[Gerrit.CHANGESET_OWNER].append(changeset_data["owner"]["username"])

            if granularity == 2:
                #TDB
                pass

            if granularity == 3:
                #TDB
                pass

        # Done in this way to have an order (and not a direct cast)
        events[Gerrit.CHANGESET_ID] = changeset[Gerrit.CHANGESET_ID]
        events[Gerrit.CHANGESET_EVENT] = changeset[Gerrit.CHANGESET_EVENT]
        events[Gerrit.CHANGESET_DATE] = changeset[Gerrit.CHANGESET_DATE]
        events[Gerrit.CHANGESET_OWNER] = changeset[Gerrit.CHANGESET_OWNER]

        return events

