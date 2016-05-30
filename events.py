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
            print item.keys()
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

