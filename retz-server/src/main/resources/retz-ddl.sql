--
--    Retz
--    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
--
--    Licensed under the Apache License, Version 2.0 (the "License");
--    you may not use this file except in compliance with the License.
--    You may obtain a copy of the License at
--
--        http://www.apache.org/licenses/LICENSE-2.0
--
--    Unless required by applicable law or agreed to in writing, software
--    distributed under the License is distributed on an "AS IS" BASIS,
--    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--    See the License for the specific language governing permissions and
--    limitations under the License.
--

-- see io.github.retz.protocol.data.User
CREATE TABLE users (
    key_id VARCHAR(32) NOT NULL UNIQUE,
    secret VARCHAR(64) NOT NULL,
    enabled BOOLEAN NOT NULL,
    json TEXT NOT NULL,
    PRIMARY KEY (key_id)
);

-- see io.github.retz.protocol.data.Application
CREATE TABLE applications (
    appid VARCHAR(32) NOT NULL UNIQUE,
    --persistentFiles VARCHAR(1024)[] NOT NULL,
    --largeFiles VARCHAR(1024)[] NOT NULL,
    --files VARCHAR(1024)[] NOT NULL,
    -- diskMB INTEGER,
    -- user VARCHAR(32),
    owner VARCHAR(32) NOT NULL,
    -- container VARCHAR(512) NOT NULL,
    json TEXT NOT NULL,
    PRIMARY KEY (appid)
    -- FOREIGN KEY (owner) REFERENCES users(key_id)
);

-- see io.github.retz.protocol.data.Job
CREATE TABLE jobs(
    name VARCHAR(32),
    id INTEGER NOT NULL UNIQUE,
    appid varchar(32) not null,
    cmd varchar(1024) not null,
    priority INTEGER NOT NULL,

    started VARCHAR(32),
    finished VARCHAR(32),
    taskid VARCHAR(128), -- this introduces NULL'd index {shrug}
    state VARCHAR(16) NOT NULL,

    json TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX priority ON jobs(priority);
CREATE INDEX taskid ON jobs(taskid);
CREATE INDEX started ON jobs(started);
CREATE INDEX finished ON jobs(finished);

-- System wide properties such ad FrameworkID; see io.github.retz.dao.Property
CREATE TABLE properties(
    key VARCHAR(128) NOT NULL UNIQUE,
    value VARCHAR(256) NOT NULL,
    epoch INTEGER NOT NULL -- probably later use
);

CREATE INDEX key ON properties(key);
