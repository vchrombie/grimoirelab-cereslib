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


import pandas

class Filter(object):
    """ Class that filters information for a given dataset.

    Given a dataframe, there are cases where some information is
    not needed any more. Performance purposes or smaller datasets
    could be part of the reason for this type of needs.

    Thus, this would return a subset of the rows of a given
    dataset as a result of the filtering process.

    An example of this filtering process would be the removal of
    certain commits that follow some pattern such as merges
    or automatically generated ones.
    """

class FilterRows(Filter):
    """ Class used to filter those files that are part of a merge
    Those are initially detected as having no file or no action
    in the list of attributes. This class removes those lines.
    """

    def __init__(self, data):
        """ Main constructor of the class

        :param data: Data frame to be filtered
        :type data: pandas.DataFrame
        """

        self.data = data

    def filter_(self, columns, value):
        """ This method filter some of the rows where the 'value'
        is found in each of the 'columns'.

        :param column: list of strings
        :param value: any type

        :returns: filtered dataframe
        :rtype: pandas.DataFrame
        """

        for column in columns:
            if column not in self.data.columns:
                return self.data

        for column in columns:
            self.data = self.data[self.data[column] != value]

        return self.data

