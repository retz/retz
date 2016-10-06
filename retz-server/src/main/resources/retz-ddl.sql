--
--    Retz
--    Copyright (C) 2016 Nautilus Technologies, Inc.
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

    taskid VARCHAR(128), -- this introduces NULL'd index {shrug}
    state VARCHAR(16) NOT NULL,

    json TEXT NOT NULL,
    -- scheduled VARCHAR(32),
    -- started VARCHAR(32),
    -- finished VARCHAR(32),
    -- props Map<String, String>
    -- result INTEGER,
    -- url VARCHAR(512),
    -- reason VARCHAR(512),
    -- retry INTEGER,
    -- cpu INTEGER NOT NULL,
    -- memMB INTEGER NOT NULL,
    -- gpu INTEGER NOT NULL,
    -- state VARCHAR(16) NOT NULL,
    -- trustPVFiles BOOLEAN NOT NULL,
    PRIMARY KEY (id)
    -- FOREIGN KEY (application) REFERENCES applications(name)
);

CREATE INDEX taskid ON jobs(taskid)