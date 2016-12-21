For assignment 7, you'll read user sessions (from Avro container file), apply a filter, and output these user sessions into different categories based on the type of each user session. The output to different categories will be accomplished using multiple outputs from each mapper, so this will be a map-only job.

User session categories are defined as follows:

SUBMITTER - session has any of these events: CHANGE or EDIT CONTACT_FORM
CLICKER - Not a Submitter session, has a CLICK  event
SHOWER - Not a Clicker session, has a SHOW or DISPLAY event
VISITOR - Not a Shower session, has a VISIT event
OTHER - None of the above.
Required elements:

Create user sessions from the file provided: dataSet7.tsv
Filter out all sessions with more than 100 events.
There are an uneven number of sessions in the categories, so in order to make the number of sessions output in each category you will implement filters that randomly sample sessions as follows:
CLICKER sessions - sample 1 in 10 (or 10%)
SHOWER sessions - sample 1 in 50 (2%)
Keep all sessions of type SUBMITTER, VISITOR, and OTHER
Add counters that record the number of sessions that were filtered out. You'll need three such counters, one for large sessions (> 100 events), one for CLICKER sessions, and one for SHOWER sessions.
Use the AvroMultipleOutputs class to output sessions to different files based on category.
You will need to determine the category for each session using the definition of session category given above.
Use the getText() method of enum SessionType (provided on Canvas) to the get the name for each "named output" used with the AvroMultipleOutputs class.
Map-only job, so set the number of reducers to zero.
Use AvroKeyValueOutputFormat for your output format.
Use the setCountersEnabled() method of AvroMultipleOutputs to create counters to count the number of each session type that you write out. These counts will be reported in the syslog file, which you will turn in for this assignment.
