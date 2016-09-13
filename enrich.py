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

import numpy as np

class Enrich(object):
    """ Class that enriches information for a given dataset.

    Its usual way of working consists of providing a new columns
    in the dataframe from a given entry. This allows to keep
    adding extra information to the basics coming from the original
    data source.

    An example of this would be the addition of the affiliation,
    gender, workload adequacy and other metrics for a given commmit.
    """


class PairProgramming(Enrich):
    """ This class splits a commit into 2 of them according to the author name.

    There are cases where two developers have participated in a commit.
    One of the ways to identify those is claiming that commit as author, but
    also as a committer.

    Depending on the process of the organization, this may indicate a review
    process, a peer development process or even nothing.

    This class should be called when committer an author usually participate
    in the development of such commit.
    """

    def __init__(self, commits):
        """ Main constructor of the class

        :param commits: original list of commits
        :type commits: dataframe with commit information
        """

        self.commits = commits

    def enrich(self, column1, column2):
        """ This class splits those commits where column1 and column2
        values are different

        :param column1: column to compare to column2
        :param column2: column to compare to column1
        :type column1: string
        :type column2: string

        :returns: self.commits with duplicated rows where the values at
            columns are different. The original row remains while the second
            row contains in column1 and 2 the value of column2.
        :rtype: pandas.DataFrame
        """

        if column1 not in self.commits.columns or \
           column2 not in self.commits.columns:
            return self.commits

        # Select rows where values in column1 are different from
        # values in column2
        pair_df = self.commits[self.commits[column1]!=self.commits[column2]]
        new_values = list(pair_df[column2])
        # Update values from column2
        pair_df[column1] = new_values

        # This adds at the end of the original dataframe those rows duplicating
        # information and updating the values in column1
        return self.commits.append(pair_df)


class FileType(Enrich):
    """ This class creates a new column with the file type
    """

    def __init__(self, data):
        """ Main constructor of the class where the original dataframe
        is provided

        :param data: original dataframe
        : type data: pandas.DataFrame
        """

        self.data = data

    def enrich(self, column):
        """ This method adds a new column depending on the extension
        of the file.

        :param column: column where the file path is found
        :type column: string

        :return: returns the original dataframe with a new column named as
                 'filetype' that contains information about its extension
        :rtype: pandas.DataFrame
        """

        if column not in self.data:
            return self.data

        # Insert a new column with default values
        self.data["filetype"] = 'Other'

        # Insert 'Code' only in those rows that are
        # detected as being source code thanks to its extension
        reg = "\.c$|\.h$|\.cc$|\.cpp$|\.cxx$|\.c\+\+$|\.cp$"
        self.data.loc[self.data[column].str.contains(reg)==True, 'filetype'] = 'Code'

        return self.data


class Gender(Enrich):
    """ This class creates three new columns with the gender of
    the name provided
    """


    def __init__(self, data, key=None):
        """ Main constructor of the class where the original dataframe
        is provided.

        :param data: original dataframe
        :param key: genderize key (optional)
        :type data: pandas.DataFrame
        :type key: string
        """

        from genderize import Genderize

        self.data = data
        self.gender = {} # init the name-gender dictionary
        self.key = key

        # Init the genderize connection
        self.connection = Genderize()
        if self.key:
            self.connection = Genderize(api_key=self.key)

    def enrich(self, column):
        """ This method calculates thanks to the genderize.io API the gender
        of a given name.

        This method initially assumes that for the given
        string, only the first word is the one containing the name
        eg: Daniel Izquierdo <dizquierdo@bitergia.com>, Daniel would be the name.

        If the same class instance is used in later gender searches, this stores
        in memory a list of names and associated gender and probability. This is
        intended to have faster identifications of the gender and less number of
        API accesses.

        :param column: column where the name is found
        :type column: string

        :return: original dataframe with four new columns:
         * gender: male, female or unknown
         * gender_probability: value between 0 and 1
         * gender_count: number of names found in the Genderized DB
         * gender_analyzed_name: name that was sent to the API for analysis
        :rtype: pandas.DataFrame
        """

        if column not in self.data.columns:
            return self.data

        splits = self.data[column].str.split(" ")
        splits = splits.str[0]
        self.data["gender_analyzed_name"] = splits
        self.data["gender_probability"] = -1.0
        self.data["gender"] = "-"
        self.data["gender_count"] = -1

        names = list(self.data["gender_analyzed_name"].unique())

        #TODO Code for calling 10 names at the same time
        #for ten_names in range(0, len(names), 10):
            #gender_result = self.connection.get(names[ten_names:ten_names+10])
            #for name in names[ten_names:ten_names+10]:
        for name in names:
            if name in self.gender.keys():
                gender_result = self.gender[name]
            else:
                try:
                    #TODO: some errors found due to encode utf-8 issues.
                    #Adding a try-except in the meantime.
                    gender_result = self.connection.get([name])[0]
                except:
                    continue

                #Store info in the list of users
                self.gender[name] = gender_result

            #Update current dataset    
            self.data.loc[self.data["gender_analyzed_name"]==name, 'gender'] = gender_result["gender"]
            if "probability" in gender_result.keys():
                self.data.loc[self.data["gender_analyzed_name"]==name, 'gender_probability'] = gender_result["probability"]
                self.data.loc[self.data["gender_analyzed_name"]==name, 'gender_count'] = gender_result["count"]

        return self.data


class TimeDifference(Enrich):
    """ This class creates a new column with the difference in seconds
    between two dates.
    """

    def __init__(self, data):
        """ Main constructor of the class where the original dataframe
        is provided

        :param data: original dataframe
        :type data: pandas.DataFrame
        """

        self.data = data

    def enrich(self, column1, column2):
        """ This method calculates the difference in seconds between
            the 2 columns (column2 - column1)

        The final result may provided negative values depending on the values
        from column1 and column2.

        :param column1: first column. Values in column1 must be datetime type
        :param column2: second column. Values in column2 must be datetime type
        :type column1: string
        :type column2: string

        :return: original dataframe with a new column with the difference
            between column2 - column1
        :rtype: pandas.DataFrame
        """

        if column1 not in self.data.columns or \
           column2 not in self.data.columns:
            return self.data

        self.data["timedifference"] = (self.data[column2] - self.data[column1]) / np.timedelta64(1, 's')
        return self.data
