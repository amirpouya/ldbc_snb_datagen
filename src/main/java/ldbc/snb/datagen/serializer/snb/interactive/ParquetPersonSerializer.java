/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */


package ldbc.snb.datagen.serializer.snb.interactive;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.HDFSCSVWriter;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.Pair;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.lang.String;
import java.io.File;
import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.htrace.fasterxml.jackson.databind.node.POJONode;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import parquet.Log;
import parquet.Preconditions;
import parquet.column.page.PageReadStore;
import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordReader;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import scala.runtime.StringAdd;


public class ParquetPersonSerializer extends PersonSerializer {

    private ParquetWriter<GenericRecord>[] writers;
    private  HashMap SchemasRawStrings;

    private enum SchemaNames {
        PERSON ("person"),
        PERSON_SPEAKS_LANGUAGE ("person_speaks_language"),
        PERSON_HAS_EMAIL ("person_email_emailaddress"),
        PERSON_LOCATED_IN_PLACE ("person_isLocatedIn_place"),
        PERSON_HAS_INTEREST_TAG ("person_hasInterest_tag"),
        PERSON_WORK_AT ("person_workAt_organisation"),
        PERSON_STUDY_AT ("person_studyAt_organisation"),
        PERSON_KNOWS_PERSON("person_knows_person");

        private final String name;

        private SchemaNames( String name ) {
            this.name = name;
        }
        public String toString() {
            return name;
        }
    }

    public ParquetPersonSerializer() {
    }

    public void initialize(Configuration conf, int reducerId) {
        int numSchemas = SchemaNames.values().length;
        Schema[] schemas = new Schema[numSchemas];

        MessageType schema = MessageTypeParser.parseMessageType("message m { required group a {required binary b;} required group c { required int64 d; }}");

        schemas[SchemaNames.PERSON.ordinal()] = ReflectData.get().getSchema(Person.class);
        schemas[SchemaNames.PERSON_KNOWS_PERSON.ordinal()] = ReflectData.get().getSchema(Knows.class);
        schemas[SchemaNames.PERSON_STUDY_AT.ordinal()] = ReflectData.get().getSchema(StudyAt.class);
        schemas[SchemaNames.PERSON_WORK_AT.ordinal()] = ReflectData.get().getSchema(WorkAt.class);
        schemas[SchemaNames.PERSON_HAS_INTEREST_TAG.ordinal()] = ReflectData.get().getSchema(Tag.class);
        for( int i = 0; i < numSchemas; ++i) {
            //TODO: Change this path;
            String parquetFile = "hdfs://localhost/tmp/parquet/data.parquet";
            Path path = new Path(parquetFile);

            writers[i] = new ParquetWriter(path,)
        }


        // Pair <Name, Type>

        ArrayList<Pair<String,String>> arguments = new ArrayList<>();
        arguments.add(new Pair<String, String >("id","r_int"));
        arguments.add(new Pair<String, String >("firstName","string"));
        arguments.add(new Pair<String, String >("lastName","string"));
        arguments.add(new Pair<String, String >("gender","string"));
        arguments.add(new Pair<String, String >("birthday","string"));
        arguments.add(new Pair<String, String >("creationDate","string"));
        arguments.add(new Pair<String, String >("locationIP","string"));
        arguments.add(new Pair<String, String >("browserUsed","string"));
        SchemasRawStrings.put(SchemaNames.PERSON.ordinal(),arguments);

        arguments.clear();
        arguments.add(new Pair<String, String >("Person.id","int"));
        arguments.add(new Pair<String, String >("language","string"));
        SchemasRawStrings.put(SchemaNames.PERSON_SPEAKS_LANGUAGE.ordinal(),arguments);

        arguments.clear();
        arguments.add(new Pair<String, String>("Person.id","int"));
        arguments.add(new Pair<String, String>("email","string"));
        SchemasRawStrings.put(SchemaNames.PERSON_HAS_EMAIL.ordinal(),arguments);

        arguments.clear();
        arguments.add(new Pair<String, String>("Person.id","int"));
        arguments.add(new Pair<String, String>("Place.id","int"));

        SchemasRawStrings.put(SchemaNames.PERSON_LOCATED_IN_PLACE.ordinal(),arguments);

        arguments.clear();
        arguments.add(new Pair<String, String>("Person.id","int"));
        arguments.add(new Pair<String, String>("Tag.id","int"));
        SchemasRawStrings.put(SchemaNames.PERSON_HAS_INTEREST_TAG.ordinal(),arguments);

        arguments.clear();
        arguments.add(new Pair<String, String>("Person.id","int"));
        arguments.add(new Pair<String, String>("Organisation.id","int"));
        arguments.add(new Pair<String, String>("workFrom","string"));
        SchemasRawStrings.put(SchemaNames.PERSON_WORK_AT.ordinal(),arguments);

        arguments.clear();
        arguments.add(new Pair<String, String>("Person.id","int"));
        arguments.add(new Pair<String, String>("Organisation.id","int"));
        arguments.add(new Pair<String, String>("classYear","int"));
        SchemasRawStrings.put(SchemaNames.PERSON_STUDY_AT.ordinal(),arguments);

        arguments.clear();
        arguments.add(new Pair<String, String>("Person1","r_int"));
        arguments.add(new Pair<String, String>("Person2","r_int"));
        arguments.add(new Pair<String, String>("creationDate","Date"));
        SchemasRawStrings.put(SchemaNames.PERSON_KNOWS_PERSON.ordinal(),arguments);
        */

    }

    @Override
    public void close() {
        int numFiles = SchemaNames.values().length;
        for(int i = 0; i < numFiles; ++i) {
            // Nothing
        }
        return;

    }

    @Override
    protected void serialize(final Person p) {

        ArrayList<String> arguments = new ArrayList<String>();

        arguments.add(Long.toString(p.accountId()));
        arguments.add(p.firstName());
        arguments.add(p.lastName());
        if(p.gender() == 1) {
            arguments.add("m");
        } else {
            arguments.add("f");
        }

        String dateString = Dictionaries.dates.formatDate(p.birthDay());
        arguments.add(dateString);

        dateString = Dictionaries.dates.formatDateTime(p.creationDate());
        arguments.add(dateString);
        arguments.add(p.ipAddress().toString());
        arguments.add(Dictionaries.browsers.getName(p.browserId()));
        writers[SchemaNames.PERSON.ordinal()].writeEntry(arguments);

        return;

        ArrayList<Integer> languages = p.languages();
        for (int i = 0; i < languages.size(); i++) {
            arguments.clear();
            arguments.add(Long.toString(p.accountId()));
            arguments.add(Dictionaries.languages.getLanguageName(languages.get(i)));
            writers[FileNames.PERSON_SPEAKS_LANGUAGE.ordinal()].writeEntry(arguments);
        }

        Iterator<String> itString = p.emails().iterator();
        while (itString.hasNext()) {
            arguments.clear();
            String email = itString.next();
            arguments.add(Long.toString(p.accountId()));
            arguments.add(email);
            writers[FileNames.PERSON_HAS_EMAIL.ordinal()].writeEntry(arguments);
        }

        arguments.clear();
        arguments.add(Long.toString(p.accountId()));
        arguments.add(Integer.toString(p.cityId()));
        writers[FileNames.PERSON_LOCATED_IN_PLACE.ordinal()].writeEntry(arguments);

        Iterator<Integer> itInteger = p.interests().iterator();
        while (itInteger.hasNext()) {
            arguments.clear();
            Integer interestIdx = itInteger.next();
            arguments.add(Long.toString(p.accountId()));
            arguments.add(Integer.toString(interestIdx));
            writers[FileNames.PERSON_HAS_INTEREST_TAG.ordinal()].writeEntry(arguments);*/
        }
    }

    @Override
    protected void serialize(final StudyAt studyAt) {
        ArrayList<String> arguments = new ArrayList<String>();
        String dateString = Dictionaries.dates.formatYear(studyAt.year);
        arguments.add(Long.toString(studyAt.user));
        arguments.add(Long.toString(studyAt.university));
        arguments.add(dateString);
        writers[FileNames.PERSON_STUDY_AT.ordinal()].writeEntry(arguments);
    }

    @Override
    protected void serialize(final WorkAt workAt) {
        ArrayList<String> arguments = new ArrayList<String>();
        String dateString = Dictionaries.dates.formatYear(workAt.year);
        arguments.add(Long.toString(workAt.user));
        arguments.add(Long.toString(workAt.company));
        arguments.add(dateString);
        writers[FileNames.PERSON_WORK_AT.ordinal()].writeEntry(arguments);
    }

    @Override
    protected void serialize(final Person p, Knows knows) {
        ArrayList<String> arguments = new ArrayList<String>();
        String dateString = Dictionaries.dates.formatDateTime(knows.creationDate());
        arguments.add(Long.toString(p.accountId()));
        arguments.add(Long.toString(knows.to().accountId()));
        arguments.add(dateString);
        writers[FileNames.PERSON_KNOWS_PERSON.ordinal()].writeEntry(arguments);
    }

    @Override
    public void reset() {

    }
}
