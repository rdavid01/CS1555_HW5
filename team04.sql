-- David Reidenbaugh (dmr117)
-- Eric Copeland (ejc76)

DROP TABLE IF EXISTS FOREST CASCADE;
DROP TABLE IF EXISTS STATE CASCADE;
DROP TABLE IF EXISTS COVERAGE CASCADE;
DROP TABLE IF EXISTS ROAD CASCADE;
DROP TABLE IF EXISTS INTERSECTION CASCADE;
DROP TABLE IF EXISTS WORKER CASCADE;
DROP TABLE IF EXISTS SENSOR CASCADE;
DROP TABLE IF EXISTS REPORT CASCADE;
DROP TABLE IF EXISTS EMERGENCY CASCADE;

DROP DOMAIN IF EXISTS energy_dom;
CREATE DOMAIN energy_dom AS integer CHECK (value >= 0 AND value <= 100);

CREATE TABLE FOREST (
    forest_no       varchar(10),
    name            varchar(30) NOT NULL,
    area            real NOT NULL,
    acid_level      real NOT NULL,
    mbr_xmin        real NOT NULL,
    mbr_xmax        real NOT NULL,
    mbr_ymin        real NOT NULL,
    mbr_ymax        real NOT NULL,
    sensor_count    integer DEFAULT 0,  --new attribute sensor_count
    CONSTRAINT FOREST_PK PRIMARY KEY (forest_no),
    CONSTRAINT FOREST_UN1 UNIQUE (name) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT FOREST_UN2 UNIQUE (mbr_xmin, mbr_xmax, mbr_ymin, mbr_ymax) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT FOREST_CH CHECK (acid_level >= 0 AND acid_level <= 1)
);


CREATE TABLE STATE (
    name            varchar(30) NOT NULL,
    abbreviation    varchar(2),
    area            real NOT NULL,
    population      integer NOT NULL,
    CONSTRAINT STATE_PK PRIMARY KEY (abbreviation),
    CONSTRAINT STATE_UN UNIQUE (name) DEFERRABLE INITIALLY DEFERRED
);


CREATE TABLE COVERAGE (
    forest_no       varchar(10),
    state           varchar(2),
    percentage      real NOT NULL,
    area            real NOT NULL,
    CONSTRAINT COVERAGE_PK PRIMARY KEY (forest_no, state),
    CONSTRAINT COVERAGE_FK1 FOREIGN KEY (forest_no) REFERENCES FOREST(forest_no) DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT COVERAGE_FK2 FOREIGN KEY (state) REFERENCES STATE(abbreviation) DEFERRABLE INITIALLY IMMEDIATE
);


CREATE TABLE ROAD (
    road_no varchar(10),
    name    varchar(30) NOT NULL,
    length  real NOT NULL,
    CONSTRAINT ROAD_PK PRIMARY KEY (road_no)
);



CREATE TABLE INTERSECTION (
    forest_no varchar(10),
    road_no   varchar(10),
    CONSTRAINT INTERSECTION_PK PRIMARY KEY (forest_no, road_no),
    CONSTRAINT INTERSECTION_FK1 FOREIGN KEY (forest_no) REFERENCES FOREST(forest_no) DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT INTERSECTION_FK2 FOREIGN KEY (road_no) REFERENCES ROAD(road_no) DEFERRABLE INITIALLY IMMEDIATE
  );


CREATE TABLE WORKER (
    ssn  varchar(9) ,
    name varchar(30) NOT NULL,
    rank integer NOT NULL,
    employing_state varchar(2) NOT NULL,
    CONSTRAINT WORKER_PK PRIMARY KEY (ssn),
    CONSTRAINT WORKER_UN UNIQUE (name) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT WORKER_FK FOREIGN KEY (employing_state) REFERENCES STATE(abbreviation) DEFERRABLE INITIALLY IMMEDIATE
);


CREATE TABLE SENSOR
  (
    sensor_id integer,
--     sensor_id serial,
    x real NOT NULL,
    y real NOT NULL,
    last_charged timestamp NOT NULL,
    maintainer   varchar(9) DEFAULT NULL,
    last_read    timestamp NOT NULL,
    energy energy_dom NOT NULL,
    CONSTRAINT SENSOR_PK PRIMARY KEY (sensor_id),
    CONSTRAINT SENSOR_FK FOREIGN KEY (maintainer) REFERENCES WORKER(ssn) DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT SENSOR_UN UNIQUE (x, y) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE REPORT (
    sensor_id integer,
    report_time timestamp NOT NULL,
    temperature real NOT NULL,
    CONSTRAINT REPORT_PK PRIMARY KEY (sensor_id, report_time),
    CONSTRAINT REPORT_FK FOREIGN KEY (sensor_id) REFERENCES SENSOR(sensor_id) DEFERRABLE INITIALLY IMMEDIATE
);

CREATE TABLE EMERGENCY ( --new table emergency
  sensor_id integer,
  report_time timestamp NOT NULL,
  CONSTRAINT EMERGENCY_PK PRIMARY KEY (sensor_id, report_time),
  CONSTRAINT EMERGENCY_FK FOREIGN KEY (sensor_id, report_time) REFERENCES REPORT(sensor_id, report_time) DEFERRABLE INITIALLY IMMEDIATE
);



--required functions and procedures ====================================================================================
--stored procedure: incrementSensorCount_proc
    --checks which forests contain a sensor (given x,y coordinates) and then increments the count
create or replace procedure incrementSensorCount_proc(sensor_x real, sensor_y real)--, inout retSensors integer)
    as
    $$
    declare
        recForest record;
        --cursor selects info for forests that contain the sensor
        currForest cursor
            FOR SELECT forest_no, mbr_xmin, mbr_xmax, mbr_ymin, mbr_ymax, sensor_count
                FROM forest as F
                WHERE sensor_x between mbr_xmin and mbr_xmax and
                      sensor_y between mbr_ymin and mbr_ymax;
    begin
        open currForest;

        --increment the count for forests containing the newly inserted sensor
        loop
            fetch currForest into recForest;
            exit when not found;
            update forest
            set sensor_count = recForest.sensor_count + 1
            where sensor_x between mbr_xmin and mbr_xmax and
                      sensor_y between mbr_ymin and mbr_ymax;
        end loop;

        close currForest;
    end;
    $$ language plpgsql;


--function: computePercentage --------------------------------------------------------
    --returns ratio of forest coverage in a specific state to entire forest area
create or replace function computePercentage(forest_no varchar(10), area_covered real)
    returns real as
    $$
    declare
        totalArea real; --holds the forests area
        ratio real;     --holds the ratio between the forest area and coverage area
    begin
        --get value for totalArea
        select F.area into totalArea
        from forest as F
        where F.forest_no = computePercentage.forest_no;
        --find ratio
        ratio := area_covered/totalArea;
        return ratio;
    end;
    $$ language plpgsql;



--required triggers ====================================================================================================
--trigger: sensorCount_tri
    --automatically increment sensor_count for forests when new sensor added in them
create or replace function updateSensorCount()
    returns trigger as
    $$
    begin
        call incrementSensorCount_proc(new.x, new.y);
        return new;
    end;
    $$ language plpgsql;

drop trigger if exists sensorCount_tri on sensor;
create trigger sensorCount_tri
    after insert --update sensor count after so that only happens if action goes through
    on sensor
    for each row
execute procedure updateSensorCount();


--trigger: percentage_tri -------------------------------------------------------
    --automatically update coverage value for percentage when coverage area in a state is updated
create or replace function updatePercentage()
    returns trigger as
    $$
    declare
        totalCovArea real;
    begin
        --compute new percentage based on update
        select computePercentage(new.forest_no, new.area) into new.percentage;

        --check that total coverage area for a forest does not exceed actual area of a forest
            --do this by making sure that the sum of all percentages based on state is <= 1
        select sum(percentage) into totalCovArea
        from coverage
        where forest_no = new.forest_no;
        --get total percentage (after the update) for the coverage of a forest
        totalCovArea = totalCovArea - old.percentage + new.percentage;
        --rollback if coverage area percentage for a forest
        if totalCovArea > 1 then
            rollback;
        end if;

        return new;
    end;
    $$ language plpgsql;


drop trigger if exists percentage_tri on coverage;
create trigger percentage_tri
    before update  --using before to fix the mutating trigger problem that would occur if used after
    on coverage
    for each row
execute procedure updatePercentage();


--trigger: emergency_tri -------------------------------------------------
    --when report inserted with temperature > 100 degrees, insert corresponding tuple into EMERGENCY table
create or replace function addEmergency()
    returns trigger as
    $$
    begin
        insert into emergency
            values (new.sensor_id, new.report_time);
        return new;
    end;
    $$ language plpgsql;


drop trigger if exists emergency_tri on report;
create trigger emergency_tri
    after insert
    on report
    for each row
        when (new.temperature > 100)
execute procedure addEmergency();


-- trigger: enforceMaintainer_tri-------------------------------------------
--     makes sure maintainer is in same state as the sensor they service
--     sensor addition fail if outside maintainer employing state
create or replace function enforceMaintainerState()
    returns trigger as
    $$
    declare
        maintainerState varchar(2);
        validState boolean := false;
    begin
        --get state that updated sensor's maintainer works in, store in maintainerState
        select employing_state into maintainerState
        from worker
        where ssn = new.maintainer;

        --validState becomes true if the updated/inserted sensor could be in the maintainer's state
        select (state = maintainerState) into validState
        from (
                --select the states within which the sensor could potentially be
                 select distinct state
                 from coverage as C
                 where C.forest_no in (
                     --get the forests within which the sensor lies
                     select distinct F.forest_no
                     from forest as F
                              join sensor as S
                                   on (new.X between MBR_XMin and MBR_XMax)
                                       and (new.Y between MBR_YMin and MBR_YMax)
                     where sensor_id = new.sensor_id
                 )
             ) as acceptableStates
        where state = maintainerState;

        --rollback the entire transaction if the maintainer's state is not valid
        if validState is null and maintainerState is not null then
            rollback;
        end if;

        return new;
    end;
    $$ language plpgsql;


drop trigger if exists enforceMaintainer_tri on sensor;
create trigger enforceMaintainer_tri
    after insert or update  --include update for case when switching sensors maintained by two workers
    on sensor
    for each row
execute procedure enforceMaintainerState();




--additional helper triggers/functions/procedures ======================================================================

--trigger to auto increment forest number
create or replace function chooseNextForestNum()
    returns trigger as
    $$
    declare
        maxForestNum integer;
    begin
        --get greatest forest_num in table
        select max(cast(forest_no as integer)) into maxForestNum
        from forest;

        --if there are no forests yet, set forest_no to 1 (it is the first)
        if maxForestNum is null then
            new.forest_no = 1;
        else
            new.forest_no = maxForestNum+1; --assign next higher forest_no
        end if;

        return new;
    end;
    $$ language plpgsql;

drop trigger if exists autoIncForestNum_tri on forest;
create trigger autoIncForestNum_tri
    before insert
    on forest
    for each row
execute procedure chooseNextForestNum();


--trigger to auto increment sensor id-------------------------
create or replace function chooseNextSensorId()
    returns trigger as
    $$
    declare
        maxSensorId integer;
    begin
        --get greatest sensor_id in table
        select max(sensor_id) into maxSensorId
        from sensor;

        --if there are no sensors yet, set sensor_id to 1 (it is the first)
        if maxSensorId is null then
            new.sensor_id = 1;
        else
            new.sensor_id = maxSensorId+1; --assign next highest sensor id
        end if;

        return new;
    end;
    $$ language plpgsql;

drop trigger if exists autoIncSensorId_tri on sensor;
create trigger autoIncSensorId_tri
    before insert
    on sensor
    for each row
execute procedure chooseNextSensorId();


--trigger to make sure worker name is upper case--------
create or replace function workerNameToUpper()
    returns trigger as
    $$
        begin
            new.name := upper(new.name); --worker name contains all uppercase letters
            return new;
        end;
    $$ language plpgsql;

drop trigger if exists workerNameUpper_tri on worker;
create trigger workerNameUpper
    before insert
    on worker
    for each row
execute procedure workerNameToUpper();


--trigger to make sure forest name is upper case--------
create or replace function forestNameToUpper()
    returns trigger as
    $$
        begin
            new.name := upper(new.name); --forest name contains all uppercase letters
            return new;
        end;
    $$ language plpgsql;

drop trigger if exists forestNameUpper_tri on forest;
create trigger forestNameUpper
    before insert
    on forest
    for each row
execute procedure forestNameToUpper();




-- views ===============================================================================================================
--sensor_states contains the sensor_id, state, and maintainer of each sensor
    --if a sensor is in multiple states, it will appear once in the view for each state
    --used to optimize switch maintainer functionality
create or replace view SENSOR_STATES as
    SELECT distinct Sensor_Id, c.state, maintainer
    FROM FOREST as F
         JOIN SENSOR as S
              ON (X between MBR_XMin and MBR_XMax) and (Y between MBR_YMin and MBR_YMax)
         join coverage as C on F.forest_no = c.forest_no;
