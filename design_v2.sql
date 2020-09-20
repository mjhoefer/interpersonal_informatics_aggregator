--
-- File generated with SQLiteStudio v3.2.1 on Fri Sep 18 20:12:43 2020
--
-- Text encoding used: System
--
PRAGMA foreign_keys = off;
BEGIN TRANSACTION;

-- Table: attachments
CREATE TABLE attachments (attachment_id INTEGER PRIMARY KEY UNIQUE NOT NULL, attachment_blob BLOB, attachment_extension TEXT, fk_message_id INTEGER REFERENCES messages (message_id));

-- Table: audio
CREATE TABLE audio (audio_id INTEGER PRIMARY KEY UNIQUE NOT NULL, audio_blob BLOB, audio_extension TEXT, audio_transcript TEXT);

-- Table: identities
CREATE TABLE identities (platform_identifier TEXT, identity_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, fk_person_id INTEGER REFERENCES persons (person_id) ON DELETE CASCADE ON UPDATE CASCADE);

-- Table: messages
CREATE TABLE messages (fk_from_id INTEGER REFERENCES identities (identity_id) NOT NULL, fk_to_id INTEGER REFERENCES identities (identity_id), message_id INTEGER PRIMARY KEY UNIQUE NOT NULL, message_timestamp TIME, message_text TEXT, has_photo BOOLEAN, has_audio BOOLEAN, has_other_attachment BOOLEAN);

-- Table: persons
CREATE TABLE persons (person_name TEXT, person_is_self BOOLEAN DEFAULT (0), person_id INTEGER PRIMARY KEY NOT NULL UNIQUE);

-- Table: photos
CREATE TABLE photos (photo_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, photo_blob BLOB, fk_message_id INTEGER REFERENCES messages (message_id) NOT NULL, photo_extension TEXT);

-- Table: platforms
CREATE TABLE platforms (platform_id INTEGER PRIMARY KEY NOT NULL UNIQUE, platform_name TEXT);

COMMIT TRANSACTION;
PRAGMA foreign_keys = on;
