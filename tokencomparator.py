# tokencamparator.py

# This program checks whether a Cassandra 
# token is within a specific range: 
# start-of-range <= token-of-interest <=  end-of-range

# list1 are values you want to check,
# create an object first: suspects = Members.member_file('./member'). 
# This takes a file with tokens indented by a new line.

# list2 are ranges against which you want to check, create an object first: 
# checkagainst = KeyspaceRanges.keyspace_stdout('./listofranges'). 
# This takes in a raw output from nodetool describering <keyspace>.

# Call the worker function check(list1, list2), e.g. check(suspect, checkagainst)

# WARNING, TODO: The node with lowest token owns the range less than or equal to its token and the range greater than the highest token, which is also known as the “wrapping range. This script does not account for the wrapping range.

def check(list1, list2):
    for suspect in list1:
        print('Suspect token {}'.format(suspect.membertoken))
        for suspectrange in list2:
            if start(suspectrange.token1, suspectrange.token2) <= suspect.membertoken <= end(suspectrange.token1,suspectrange.token2):
                print('Token {} belongs between {} and {}'.format(suspect.membertoken,  suspectrange.token1, suspectrange.token2))

def start(token1, token2):
    if token1 < token2:
        start_token = token1
    else:
        start_token = token2
    return (start_token)

def end(token1, token2):
    if token1 > token2:
        end_token = token1
    else:
        end_token = token2
    return (end_token)

class RangeSlice(object):
    def __init__(self, token1, token2):
        self.token1 = token1
        self.token2 = token2

    def __repr__(self):
        return 'Rangeslice({!r},{!r})'.format(self.token1, self.token2)

class KeyspaceRanges(object):
    def __init__(self):
        self.slices = []

    def __getattr__(self, name):
        return getattr(self.slices, name)
    
    @classmethod
    def keyspace_stdout(cls, filename):
        self = cls()
        with open(filename, 'r') as f:
            next(f)
            next(f)
            for row in f:
                rowlist = row.split(',')
                start = rowlist[0].split(':')
                end = rowlist[1].split(':')
                h = RangeSlice(int(start[1]), int(end[1]))
                self.slices.append(h)
            return self

    def __len__(self):
        return len(self.slices)

    def __iter__(self):
        return self.slices.__iter__()

class Member(object):
    def __init__(self, membertoken):
        self.membertoken = membertoken

    def __repr__(self):
        return 'Token:({!r})'.format(self.membertoken)

class Members(object):
    def __init__(self):
        self.members = []

    def __getattr__(self, name):
        return getattr(self.members, name)

    @classmethod
    def member_file(cls, filename):
        self = cls()
        with open(filename, 'r') as f:
            for row in f:
                h = Member(int(row.rstrip("\n\r")))
                self.members.append(h)
            return self
    
    def __len__(self):
        return len(self.members)

    def __iter__(self):
        return self.members.__iter__()
