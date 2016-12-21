For Assignment 5, you'll define an Avro object that represents a user session. Your mapReduce job will read in individual log entries, create an Event object that represents each log entry, and assemble these into user sessions. A session should be created for each unique userId, as the userId identifies activities by the same user.

You should download dataSet5Small.tsv and dataSet5Header.tsv from Canvas and examine the content to familiarize yourself with what is represented in a log entry. The log files are tab separated (.TSV), and dataSet5Header.tsv defines the fields of an individual log entry.

Your first task is to run WordCount against dataSet5.tsv to get a list of the values in the various fields. The following fields have a large number of unique values, so you need not explore the values for these fields (they'll result in a large amount of output):

event_timestamp
image_count
mileage
price
referring_domain
user_id
vin
Modify your WordCount app to output the following info:

fieldname:value    count

This will show you the values that occur in each field including which fields can have no value or a null value.

Include all the fields in your Avro object. A starting definition for the Avro schema has been provided (session.avsc, on Canvas).

The event_type field of a log entry should be broken into two fields (both enums) in your Avro schema: event_type and event_subtype. The different event_type values are already defined in session.avsc. You need to fill out the possible values for the event_subtype, which is determined by the remainder of the string value in the log entry event_type field.

Also create enumerations for these fields: body_style, cab_style, vehicle_condition.  Other fields will be strings, numbers, or booleans as appropriate.

Events for a single user_id are grouped into one session, and they should be in order of event_timestamp.

Input: dataSet5.tsv

Outputs:

Field value counts using the modified WordCount
Avro text representation of user session instances your map-reduce job creates (use TextOutputFormat)
Key and value types for the session generation reducer output:
AvroKey<CharSequence>
AvroValue<Session>
Required elements:

Modified WordCount app that outputs the counts for each unique value of each field (ignore the fields listed above).
Avro schema representing a user session.
Include all fields defined in dataSet5Header.tsv
User sessions have an array of events, sorted by event time.
The values in the features field (":" separated) should be extracted and placed in an array, in sorted order.
The fields body_style, cab_style, and vehicle_condition should be enums.
Key and value types for the session generation reducer output:
AvroKey<CharSequence>
AvroValue<Session>
Remove duplicate events.
