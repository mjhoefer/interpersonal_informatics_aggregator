from pathlib import Path
import os
from bs4 import BeautifulSoup

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker, mapper, aliased

#from ORMObjects import *

import datetime

# for automated string parsing of dates with UTC offset
from dateutil.parser import parse

# for contact parsing in csv
import csv

# for parsing phone numbers into the google voice style
import re


# for facebook message parsing with json
import json


# automatically creates a fresh database testN.sqlite3, where N is incremented each time it's ran
def create_fresh_database(db_folder):
    curr_iteration = 1

    db_file = db_folder / ('test' + str(curr_iteration) + ".sqlite3")

    while db_file.is_file():
        # keep iterating the numbers until we have a new one
        curr_iteration = curr_iteration + 1
        db_file = db_folder / ('test' + str(curr_iteration) + ".sqlite3")

    if db_file.is_file():
        db_file = db_folder / ''

    engine = sqlalchemy.create_engine('sqlite:///' + str(db_file), echo=True, connect_args={"check_same_thread": False})
    return engine


def open_existing_database(path_to_db):
    if path_to_db.is_file():
        engine = sqlalchemy.create_engine('sqlite:///' + str(path_to_db), echo=True, connect_args={"check_same_thread": False})
    else:
        engine = None
    return engine


# opens the latest iteration of the test database sequence
# largely for debugging
def open_most_recent_database(db_folder):
    curr_iteration = 1

    db_file = db_folder / ('test' + str(curr_iteration) + ".sqlite3")

    while db_file.is_file():
        # keep iterating the numbers until we have a new one
        curr_iteration = curr_iteration + 1
        db_file = db_folder / ('test' + str(curr_iteration) + ".sqlite3")

    # go until we don't see any database, and then go back a step and open that one
    db_file = db_folder / ('test' + str(curr_iteration-1) + ".sqlite3")

    return open_existing_database(db_file)



### BEGIN ORM CLASS CODE
Base = declarative_base()

class Platform(Base):
    __tablename__ = 'platforms'

    id = Column(Integer, primary_key=True)
    platform_name = Column(String, unique=True)

    identities = relationship("Identity", back_populates="platform")


class Person(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    is_self = Column(Boolean)

    identities = relationship("Identity", back_populates="person")

    def __repr__(self):
        return f'{self.name}'

class Identity(Base):
    __tablename__ = 'identities'

    id = Column(Integer, primary_key=True)
    id_string = Column(String)
    display_name = Column(String)
    person_id = Column(Integer, ForeignKey('people.id'))
    platform_id = Column(Integer, ForeignKey('platforms.id'))

    platform = relationship("Platform", back_populates='identities')
    person = relationship("Person", back_populates="identities")

    def __repr__(self):
        return f'{self.person.name}'

    #sent_messages = relationship("Message", back_populates='sender', foreign_keys = [Message.from_person_id])
    #received_messages = relationship("Message", back_populates='recipient', foreign_keys = ['to_person_id'])


class Message(Base):
    __tablename__ = 'messages'

    id = Column(Integer, primary_key=True)
    message_text = Column(String)
    timestamp = Column(DateTime)

    from_identity_id = Column(Integer, ForeignKey('identities.id'))
    to_identity_id = Column(Integer, ForeignKey('identities.id'))

    sender = relationship("Identity", back_populates='sent_messages', foreign_keys=[from_identity_id])
    recipient = relationship("Identity", back_populates="received_messages",  foreign_keys=[to_identity_id])

    def __repr__(self):
        return f'{self.message_text}'


    def export_dict(self):
        ret = {}
        ret['message_id'] = self.id
        ret['platform'] = self.sender.platform.platform_name
        ret['sender_name'] = self.sender.display_name
        ret['recipient_name'] = self.recipient.display_name
        ret['timestamp'] = str(self.timestamp)
        ret['content'] = self.message_text

        return ret


# add the reverse foreign keys... some reason this is necessay (?)
Identity.sent_messages = relationship("Message", foreign_keys = [Message.from_identity_id])


Identity.received_messages = relationship("Message",  foreign_keys = [Message.to_identity_id])

# END ORM CLASS CODE

# INSTANTIATE DATABASE IN MEMORY
engine = create_fresh_database(Path.cwd().parent / 'db')

# make the metadata
Base.metadata.create_all(engine)

# session makers
Session = sessionmaker(bind=engine)
session = Session()
# Database is created!
# Wow that was pretty easy

# START FILLING OUT THE DB

# Gets the platform object, makes one if need be
def verify_or_make_platform(platform_name, session):
    #Create/Verify the platform has been seen before
    platforms = session.query(Platform).filter(Platform.platform_name==platform_name).all()

    if len(platforms) == 0:
        # make the google voice platform if not seen before
        new_platform = Platform()
        new_platform.platform_name = platform_name
        session.add(new_platform)
        session.commit()
    else:
        new_platform = platforms[0]

    return new_platform

# checks to see if the identity exists for a given platform and identity_string
# if it doesn't, it will make one.
def get_or_create_identity(identity_string, platform_handle, session, display_name=None, immediate=False):
    # check if identity exists
    t = session.query(Identity).filter(Identity.id_string == identity_string). \
        filter(Identity.platform == platform_handle).first()
    if not t:
        # create new identity
        new_id = Identity(platform=platform_handle, id_string=identity_string, display_name=display_name)
        session.add(new_id)
        if immediate:
            session.commit()
        t = new_id
    return t


# returns a message_thread object
# with all the messages and meta-data associated parsed into the dict
# this dict will be used to construct a relational object that will be persisted
def parse_google_text_soup(soup, self_phone):
    parsed_thread = {}
    mydivs = soup.findAll("div", {"class": "tags"})
    if len(mydivs) > 1:
        print("found too many tags, messed up")
        # print ("found voicemail in", file_p.name, "")
    tags = mydivs[0].findAll("a", {"rel": "tag"})
    labels = []
    for tag in tags:
        labels.append(tag.text)

    # now figure out who should parse this file:
    if 'Text' in labels:
        # this is a text message thread
        parsed_thread['type'] = 'Text'

        # first see if it's a group message
        participants = soup.findAll("div", {"class": "hChatLog"})
        if "Group conversation with" in participants[0].text:
            parsed_thread['is_group'] = True
            print("group convo")
        else:
            parsed_thread['is_group'] = False
            # 1:1 text message

            messages = soup.findAll("div", {"class": "message"})
            # print("a text in ", file_p.name, " ", i)

            messages_data = []
            identity_strings = []
            names = []

            # parse all the message data
            for message in messages:
                # each of these should be a message

                # find a "cite" tag with a class of sender vCard to get the sender
                sender = message.find("cite", {"class": "sender"})

                # then look for an 'a' with class 'tel'
                tel = sender.find("a", {"class": "tel"})
                tel_num = tel['href']

                date_time = message.find("abbr", {"class": "dt"})['title']

                msg = {}
                msg['id_string'] = tel_num
                msg['sender_name'] = tel.text
                msg['message_text'] = message.find("q").text
                msg['date_time'] = date_time

                messages_data.append(msg)

                # add identities here
                if tel_num not in identity_strings:
                    identity_strings.append(tel_num)
                    names.append(tel.text)

                # explicitly set the self as a conversation participant
                # just in case it's a solo message and it only implies the recipient
                if self_phone not in identity_strings:
                    identity_strings.append(self_phone)
                    names.append("Me")

            parsed_thread['messages'] = messages_data
            parsed_thread['identity_strings'] = identity_strings
            parsed_thread['display_names'] = names

    elif 'Voicemail' in labels:
        parsed_thread['type'] = 'Voicemail'
        # parse voicemail
    elif 'Missed' in labels:
        parsed_thread['type'] = 'Missed'
        # parse missed call
    elif 'Recevied' in labels:
        parsed_thread['type'] = 'Received'
        # parse received call
    elif 'Placed' in labels:
        parsed_thread['type'] = 'Placed'
        # parse connected call
    else:
        print("ISSUE!!", parsed_thread)
        parsed_thread['type'] = "Unknown"

    return parsed_thread


# Massive function for loading google voice files
def load_google_voice_export(path_of_takeout_directory, session, self_phone = 'tel:+13198990838', file_limit = None):
    voice_folder = path_of_takeout_directory / 'Takeout' / 'Voice'
    calls = voice_folder / 'Calls'

    platform_handle = verify_or_make_platform('Google Voice', session)

    files = []

    # grab all files
    for entry in os.scandir(calls):
        if not entry.is_dir():
            files.append(entry)

    # set own phone number so we can identify the self, as it is often only implicitly the recipient
    # TODO get this from the high level VCF file in the future
    self_phone = self_phone

    # keep identities in local list to avoid lots of querying
    identities = []

    # begin parsing all files
    for i, file in enumerate(files):
        # get our path object
        file_p = Path(file.path)

        if file_limit:
            if i > file_limit:
                break

        if file_p.suffix == '.html':
            #print("html found", i)
            # it's a wholistic message, so parse it
            with file_p.open(mode='r', encoding='utf-8') as f_in:
                soup = BeautifulSoup(f_in)

            # parse the HTML
            parsed_thread = parse_google_text_soup(soup, self_phone)

            if parsed_thread['type'] == 'Text':
                if parsed_thread['is_group'] == False:
                    # ensure all identities are created for these messages
                    for k, identity_string in enumerate(parsed_thread['identity_strings']):
                        # function - get or create identity
                        curr_identity = get_or_create_identity(identity_string, platform_handle, session,
                                                               display_name=parsed_thread['display_names'][k])
                        identities.append(curr_identity)

                    # now finally create the messages
                    for msg in parsed_thread['messages']:
                        # get the recipient identity string (essentially, just not the sender when we only have two
                        recipient_identity_string = [id for id in parsed_thread['identity_strings'] if id != msg['id_string']]

                        if len(recipient_identity_string) == 0:
                            # if no recipient, then you sent a message to yourself
                            recipient_tel = msg['id_string']
                        else:
                            recipient_tel = recipient_identity_string[0]

                        # get sender identity object
                        # these are in the list of identities we already have in memory. No need to query.
                        sender_identity_object = [sender for sender in identities if sender.id_string == msg['id_string']][0]
                        # sender1 = session.query(Identity).filter(Identity.id_string==msg['id_string']).first()

                        recipient_identity_object = \
                        [recipient for recipient in identities if recipient.id_string == recipient_tel][0]
                        # recip1 = session.query(Identity).filter(Identity.id_string==recipient_tel).first()

                        # create python datetime object for insertion in db
                        date_obj = parse(msg['date_time'])
                        new_msg = Message(message_text=msg['message_text'],
                                          sender=sender_identity_object,
                                          recipient=recipient_identity_object,
                                          timestamp=date_obj)
                        session.add(new_msg)
            elif parsed_thread['type'] == "Voicemail":
                # parsing additional types later
                continue
    session.commit()

# get the current path, in the python directory
dir_to_scan = Path.cwd()

# move up and go to the exports
export_folder = dir_to_scan.parent / 'exports'

# BEGIN GOOGLE IMPORT

# find google folder
google_folder = export_folder / 'google'

folder_to_parse = google_folder / 'takeout'

#load_google_voice_export(folder_to_parse, session, file_limit = 2000)
load_google_voice_export(folder_to_parse, session)


### BEGIN FACEBOOK PARSE
# do the facebook parse now

# get the platform handle
facebook = verify_or_make_platform('Facebook Messenger', session)

facebook_folder = export_folder / 'facebook'

inbox_folder = facebook_folder / 'facebook-mhoefer' / 'messages' / 'inbox'

# now walk all the folders and begin to parse
dirs = []

for entry in os.scandir(inbox_folder):
    if entry.is_dir():
        dirs.append(entry)


fb_identities = []

for i, dir in enumerate(dirs):
#    if i > 50:
#        break
    for entry in os.scandir(dir):
        if entry.is_dir():
            # one day we'll parse through audio/gifs/etc
            # although these paths can be inferred from the links
            # in the json itself
            continue
        else:
            # message file
            if entry.name.endswith(".json"):
                # load JSON
                json_file = Path(entry.path)
                with json_file.open(mode='r', encoding='utf-8') as f_in:
                    f_json = json.load(f_in)
                #parse_message_json(f_json, session)
                if len(f_json['participants']) > 2:
                    # skip group messages for now
                    continue
                else:
                    # one on one messages
                    # ensure participants are in database
                    for participant in f_json['participants']:
                        curr_participant = get_or_create_identity(participant['name'], facebook, session, \
                                                                  display_name=participant['name'])
                        if curr_participant not in fb_identities:
                            fb_identities.append(curr_participant)

                    # add messages to database
                    for message in f_json['messages']:
                        new_msg = Message()

                        date_obj = datetime.datetime.fromtimestamp(message['timestamp_ms']/1000.0)

                        if message['sender_name'] == '':
                            continue

                        sender_identity_list = \
                            [sender for sender in fb_identities if sender.id_string == message['sender_name']]

                        # apparently this list can be empty in some cases... no sender?
                        if len(sender_identity_list) > 0:
                            sender_identity_object = sender_identity_list[0]
                        else:
                            sender_identity_object = None


                        recipient_name = [recipient for recipient in f_json['participants'] if recipient['name'] != message['sender_name']]

                        if not recipient_name:
                            # message to self
                            recipient_name = message['sender_name']
                        else:
                            recipient_name = recipient_name[0]['name']
                        print(message['sender_name'], " sent to ", recipient_name)
                        recipient_identity_object = \
                            [recipient for recipient in fb_identities if recipient.id_string == recipient_name][0]

                        if 'content' not in message.keys():
                            message['content'] = None


                        new_msg = Message(message_text=message['content'],
                                          sender=sender_identity_object,
                                          recipient=recipient_identity_object,
                                          timestamp=date_obj)
                        session.add(new_msg)

session.commit()


# PARSING GOOGLE CONTACT INFO
# PLAN: put the contact pieces of information in as separate identities
# then run an algorithm to create "people" from identities based on the display name or something,
# and then if the person already exists, simply link them together.


contact_folder = google_folder / 'takeout' / 'Takeout'/ 'Contacts' / 'All Contacts'
contact_file = contact_folder / 'All Contacts.csv'

# dict reader should do the trick
contacts = {}  # a dict where the key is the name, and the value is a dict with the juicy contact deets


# a helper function that will ensure we don't overwrite values in the contact dict
# if we have duplicate contacts, that have the same field (ie, both have "email 1",
# then this function will update the contact dict with a modified key (_dup_i) where i is the
# number of duplicates for this contact. Just ensures we don't overwrite any data.
def update_contact_dict(c_dict, new_key, new_value):

    #if empty value, don't update anything
    if new_value == '':
        return

    if new_key not in c_dict.keys():
        c_dict[new_key] = new_value
        return

    # no need to create duplicate values
    if new_value in c_dict.values():
        return

    test_key = new_key
    i = 1
    while test_key in c_dict.keys():
        test_key = new_key + "_dup_" + str(i)
        i = i + 1
    c_dict[test_key] = new_value
    return

# this will format the phone in the style of google voice, to make it easy to match and merge identities
# the format is tel:+13038196688
def format_phone(phone_num):
    phone_digit_list = re.findall('[0-9]+', phone_num)
    phone_digits = "".join(phone_digit_list)

    num_digits = len(phone_digits)

    if num_digits == 10:
        # just add the 1 and call it good
        return 'tel:+1' + phone_digits
    elif num_digits > 10:
        # we have enough digits, just add the tel:+
        return 'tel:+' + phone_digits
    return "" ## not enough digits, must be 319 area code LOL

## This section may break depending on the google voice contacts CSV file
## The number of phone_i may change. i could be more or less than is here...
## TODO fix this
with open(contact_file, 'r', encoding='utf-8') as infile:
    readr = csv.DictReader(infile)
    for row in readr:
        if row['Name'] == "":
            ## no name, generally just email junk, we can ignore it.
            continue
        # check for duplicates of names
        if not row['Name'] in contacts.keys():
            # name does not exist, so make a new contact dict
            contacts[row['Name']] = {}
        update_contact_dict(contacts[row['Name']], 'email_1', row['E-mail 1 - Value'])
        update_contact_dict(contacts[row['Name']], 'email_2', row['E-mail 2 - Value'])
        update_contact_dict(contacts[row['Name']], 'phone_1', format_phone(row['Phone 1 - Value']))
        update_contact_dict(contacts[row['Name']], 'phone_2', format_phone(row['Phone 2 - Value']))
        update_contact_dict(contacts[row['Name']], 'phone_3', format_phone(row['Phone 3 - Value']))
        update_contact_dict(contacts[row['Name']], 'phone_4', format_phone(row['Phone 4 - Value']))
        update_contact_dict(contacts[row['Name']], 'phone_5', format_phone(row['Phone 5 - Value']))

# END CONTACT CSV PARSING
# the contacts dict has all the info we might want

# make the google contact platform
goog_contact_platform = verify_or_make_platform("Google Contacts", session)

# now we can create a bunch of identities for each contact
for c_name, contact in contacts.items():
    # loop through each contact information
    for c_key, c_val in contact.items():
        # make the new identity, one per kvp
        new_id = Identity(platform=goog_contact_platform, id_string=c_val, display_name=c_name)
        session.add(new_id)

session.commit()

## Sweet, we now have a ton of identities that should match.

# Next, let's go through each identity, and make People for each identity

# first, grab all the identities from the database
# need to go look up how to query sqlalchemey again..

identities = session.query(Identity).filter(Identity.display_name!='').all()
len(identities)

# just checks for alphabet and space in a string
def is_name(name):
    okay_chars = '.()-'
    for c in name:
        if c.isalpha() or c.isspace() or c in okay_chars:
            continue
        else:
            return False
    return True


# loop through and create people for identities that have real display names
for identity in identities:
    # check if the display_name is a legit name
    if is_name(identity.display_name):
        # check if the person already exists
        person_obj = session.query(Person).filter(Person.name == identity.display_name).first()
        if person_obj:
            # okay, the person already exists, so just link the identity to that person
            identity.person = person_obj
        else:
            new_person = Person(name=identity.display_name, is_self = False)
            session.add(new_person)
            session.commit()
            identity.person = new_person

session.commit()

# next, go through and check to see if a id_string matches another id_string, that has a person attached
# then attach the first identity to that same person

# first grab all IDs with no match
unmatched_ids = session.query(Identity).filter(Identity.person_id == None).all()


for identity in unmatched_ids:
    # look for a match
    potential_mate = session.query(Identity).filter(Identity.person_id != None, Identity.id_string==identity.id_string).first()
    if potential_mate:
        print ("Found a mate for ", identity.id_string, " and its ", potential_mate.person.name)
        identity.person = potential_mate.person

session.commit()


# turns out many of the contacts are only in Apple Contacts... not google, for some reason... so
# we got a vcard from apple ... and just threw it into google. Let them parse that for me. New CSV Has it all!



exit()
























### WTF Was I trying to do below.

### HANDLING OF DUPLICATES
import sqlite3
from sqlite3 import Error
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def get_or_create_person(session, person_name):
    person = session.query(Person).filter(Person.name == person_name).first()

    if not person:
        person = Person(name = person_name)
        session.add(person)
    return person



engine = open_existing_database(Path.cwd().parent / 'db' / 'test68.sqlite3')

Session = sessionmaker(bind=engine)
session = Session()


# Create a Pandas dataframe to show how this might work.
my_messages = session.query(Message).filter(Message.from_identity_id == 2 or Message.from_identity_id == 235)


msgList = []
for message in my_messages:
    msgList.append(message.export_dict())


import pandas as pd

df = pd.DataFrame(msgList)
df.to_csv('testing_output.csv')



import csv
toCSV = [{'name':'bob','age':25,'weight':200},
         {'name':'jim','age':31,'weight':180}]
keys = toCSV[0].keys()
with open('people.csv', 'w', newline='')  as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(toCSV)

first = my_messages.first()

first.export_dict()


msg = my_messages.all()

TwinIdentity = aliased(Identity, name='twin_identity')

dups = session.query(Identity). \
               join(TwinIdentity, TwinIdentity.display_name == Identity.display_name).filter(TwinIdentity.platform_id != Identity.platform_id)

print(dups.all())

for dup in dups:
    print (dup.display_name, dup.id)
    person = get_or_create_person(session, dup.display_name)
    dup.person = person


session.commit()

new_person = Person(name = dup.display_name)
session.add(new_person)







sql_text = open(Path.cwd().parent / 'db' /'duplicates.sql', 'r', encoding='utf-8').read()

result = engine.execute(sql_text)


for row in result:
    # assign each matching identity to have a similar person
    print(row)
    print (row.keys())


result = engine.execute("SELECT identities.id AS identities_id, identities.id_string AS identities_id_string, "
                        "identities.display_name AS identities_display_name, identities.person_id AS "
                        "identities_person_id, identities.platform_id AS identities_platform_id ,"
                        "twin_identity.display_name, twin_identity.id_string, twin_identity.id"
                        "FROM identities JOIN identities AS twin_identity ON identities.display_name = twin_identity.display_name AND "
                        "identities.platform_id != twin_identity.platform_id GROUP BY identities_display_name "
                        "SELECT * FROM messages")




# Now we link identities based on same name.
# Wow, thank goodness for the ORM
platforms = session.query(Platform).all()

platform_id_dict = {}

for platform in platforms:
    platform_id_dict[platform] = platform.identities

aQuery = session.query(Identity, Identity).filter(Identity.id_string == Identity.id_string and Identity.platform_id != Identity.platform_id)
mySet = aQuery.all()





q = session.query(Identity, TwinIdentity)\
    .outerjoin(
                  (TwinIdentity, Identity.id_string==TwinIdentity.id_string)
              )#.filter(Identity.platform_id != TwinIdentity.platform_id)

AANOWAY = q.all()


TwinIdentity = aliased(Identity, name='twin_identity')


dups = session.query(Identity). \
               join(TwinIdentity, TwinIdentity.display_name == Identity.display_name).filter(TwinIdentity.platform_id != Identity.platform_id)



aaaaa = dups.all()


# SELECT identities.id AS identities_id, identities.id_string AS identities_id_string, identities.display_name AS identities_display_name, identities.person_id AS identities_person_id, identities.platform_id AS identities_platform_id ,
# twin_identity.display_name, twin_identity.id_string, twin_identity.id
# FROM identities JOIN identities AS twin_identity ON identities.display_name = twin_identity.display_name AND identities.platform_id != twin_identity.platform_id GROUP BY identities_display_name



# just need to construct


