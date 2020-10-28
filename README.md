#Analytics Engineer Take Home Exercise

##Introduction
This is a take home assignment for applicants to the Analytics Engineer position at Dwelo. The goal is for our team to
get a sense of your approach when wrangling tabular data in SQL. Hopefully this will be a worthwhile investment of your time by 
allowing you some freedom to experiment with a piece of a modern data stack. 

The expectation is that after satisfying the prerequisites outlined below this exercise will take between 2 and 4 hours 
depending on your familiarity with [BigQuery](https://cloud.google.com/bigquery), [dbt](https://getdbt.com), and SQL

###Prerequisites
You'll need Docker, a Google Account, and a Github account 
* Docker (https://docs.docker.com/get-docker/)
* Google (https://google.com)
* Create a Github account if you don't already have one and [fork this repo into your own space](
https://help.github.com/en/github/getting-started-with-github/fork-a-repo#fork-an-example-repository)

Since this exercise will query and manipulate tables in Google BigQuery you'll also need an invite to two BigQuery Datasets 
within the [analytics-interview](https://console.cloud.google.com/bigquery?organizationId=336398165328&project=analytics-interview) 
Google Cloud Project
* `interview_source` (the source data you'll be manipulating)
* `dev_[your_name]` (the destination dataset for the various views you'll be creating)

Access to these Datasets should have been provided ahead of reading this readme, but if something is amiss reach out to 
[Sam Peck](mailto:speck@dwelo.com) If you have access they will show up in the [BigQuery UI](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-web-ui) 
when you navigate to the above link. 

###BigQuery and dbt docs
You'll be using dbt (https://docs.getdbt.com/) to query and manipulate data in Google BigQuery (https://cloud.google.com/bigquery/docs)
dbt will also be used throughout this exercise to create and then validate the schema of the queries (models) you define and the data 
they produce

Beyond a general understanding of dbt, the docs for these two commands are important since you'll they'll be the primary 
mechanism through which you invoke dbt functionality. 
* `dbt test` (https://docs.getdbt.com/reference/commands/test)
* `dbt run` (https://docs.getdbt.com/reference/commands/run)

If this is your first exposure to dbt you'll probably wanna keep these dbt docs close by as you work through this exercise.

###What's a sync event?
If you browse the dataset `analytics-interview.interview_source` you'll notice there's a table called 
`raw_sync_events`. In an effort to give you a taste of the type of work you'd be doing in this role at Dwelo you'll be 
playing with real data from Dwelo's production systems (sampled and anonymized from internal test accounts). 

Dwelo's own Greg Cooper has [blogged](https://medium.com/dwelo-r-d/synchronicity-with-twilio-sync-4ab8c38e5780) about our use of 
Twilio Sync on our Medium blog. In this case you'll be parsing, transforming and prepping raw data from the Sync Map Updates 
described by Greg as they arrive in our BigQuery data warehouse.

The linked post is the most relevant for getting the surrounding context for the telemetry you'll be manipulating in this exercise. 
If you're someone who thrives on getting the surrounding context the other posts in the "Sync" series are recommended reading. 

###Launching your Docker container and running dbt for the first time
Now that you've got some background let's get started:
   1. Clone your forked repo
   2. Create a git branch for your changes
   3. `docker-compose up /usr/app/dbt/start.sh dev_[your_name]` (the dev_[your_name] param will ensure you are pointed 
   at the correct destination) 
   4. Once the docker container is built and launched you'll be prompted to authorize your cli session with Google Cloud. 
   _(during this step you may see `WARNING: Cannot find a quota project to add to ADC` which can be ignored)_
   5. 🐳:/usr/app/dbt# `dbt run` (this will create the views which have already been defined in the dbt/models folder of this repo)
   6. 🐳:/usr/app/dbt# `dbt test` (this will run the [schema tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests/#schema-tests) 
   defined in the schema.yml files, and the [sql tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests/#data-tests) in dbt/tests) 
   
You should expect some errors and failures on the first run of `dbt test`, part of this exercise will be getting all these tests passing! 

##The Challenge
It's a tale as old as data...on the one hand we have raw telemetry and on the other data consumers who really want to answer some basic
questions. Since this is a real world example these raw data are provided to you partially parsed, with duplicates, and some 
data type inconsistencies. You've already gotten a taste of the primary tools which will be aiding you in your quest `dbt test` 
and `dbt run`

This challenge is broken into three parts. "Bonus" items are just that--they are optional once you've completed the core task
they are associated with. If you're running low on time please skip over them--it's more important that we see your work on the next
section.

###Part One
The first part of this challenge involves a typical task you may handle as an Analytics Engineer at Dwelo. The data from `raw_sync_events`
has been staged using the views (dbt models) defined in the folder `dbt/models/staging/`. Your task is to update the queries to
 1. parse a few more fields
 2. deduplicate the events
 3. provide the SQL for `fct_command_statuses` which will reflect the outcome of each command among other data points
 
Part One of this challenge will be complete when your results of your `fct_command_statuses` match the expectations laid out in 
`dbt/models/marts/schema.yml` and `dbt test --models tag:part_one ` runs without errors or failures. Tests always run against 
BigQuery (not your local environment) so each time you want to test you'll need to: 
* `dbt run --models [a list of models
you've changed]`    
* `dbt test --models [a list of models
you've changed]` 

**Hints:**
* As you get started you'll want to focus on one model at a time and once tests are passing move on to the next.
* If you run `dbt test --models stg_commands` you'll get the failing/errored tests for `stg_commands` in particular.
* You may notice that each `.sql` file (model) will always contain a [single select statement](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/)
This is an important detail if you're new to dbt, making sure you understand why this is the case is a wise investment of time
* You can view the SQL that dbt sent to BigQuery on it's last run by navigating to `dbt/target/run/dwelo/models`
* The existing models utilize [Common Table Expressions](https://jamesrledoux.com/code/sql-cte-common-table-expressions);
think about how to adopt this same pattern as you create new queries
* The `source` column descriptions in `dbt/models/staging/schema.yml` hold the clues you'll need to parse the JSON accurately
* Models should reference sources and the other models on which they depend via the `source` or `ref` [functions](https://docs.getdbt.com/docs/guides/best-practices/#use-the-ref-function)
respectively
* For most changes you'll wanna work on your query in the [BigQuery Console](https://console.cloud.google.com/bigquery?organizationId=336398165328&project=analytics-interview).
The BigQuery Console (or any other interactive query interface) provides far quicker feedback on your syntax and results than 
`dbt run`. Once you've got the results you're looking for you can copy the query back into the `.sql` model file and replace 
table names with calls to `ref` or `source` functions 
* You're gonna need to deduplicate some of the data. Each of the stg_command models should have at most one row per `command_uuid`
(if timestamps differ among duplicates we want the earliest event by `raw_sync_events.DateCreated`) 
* If you're not sure when to commit your changes to git, a good rule of thumb is each time you get all the tests passing for a given
model. Here are some helpful tips on [writing helpful git commit messages](https://chris.beams.io/posts/git-commit/)
* Bonus: Can you consolidate SQL that is duplicated across models?

###Part Two
Building off your success in Part One, in this next part of the challenge you'll be staging `raw_users` in `dbt/models/staging` 
before producing the final models in `dbt/marts/`.

As in Part One `dbt test` and the `schema.yml` files are going to guide your approach. You're working toward zero failures/errors 
when running:

`dbt test --models tag:part_two`

**Hints:**
* Build on your experience completing Part One
* Refer to the relevant `schema.yml` files for descriptions of what each column should represent
* The tests are simply looking for the presence of certain columns--inspect the results of your models to sanity check your SQL
* You're gonna run into some data type issues here
* Bonus: Can your `met_daily_command_by_username` produce a timeseries without date gaps? 

###Part Three
Congrats, you've made it Part Three! Hopefully by now you're getting comfortable with wrangling queries and using tests (both manual
and those orchestrated by dbt) to validate your results. 

Your next assignment, should you choose to accept it, will allow you to show off what you've learned. In addition you'll get 
to play with some profoundly useful BigQuery datatypes: [ARRAY](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_type)
and [STRUCT](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type)

This is an intermediate to advanced SQL challenge. We're going to lay out the basic question we'd like to ask of the data with a suggested
first step. We'll then leave the implementation, schema, and even the tests up to you. This is a chance for you to show how you would 
incorporate the functionality of dbt and CTEs that you've leveraged so far to address this final challenge. We'll be dedicating as much 
time as possible in the interview to discussing the trade-offs and decisions you made in Part Three.

So far we've been focused on handling command telemetry; however, as you saw from Greg's Medium post the devices being commanded also produce
sensor readings whenever their device state changes (ie: Locked/Unlocked). After a command gets to a device, that device will adjust it's state 
to reflect the desired state dictated by the command. In most successful cases a device will produce a reading within 15 seconds of a Command.

The basic question we'd like to ask is this: **Can we associate commands with the resultant readings?**

The deliverable we're looking for is a new model in `dbt/models/marts`. A suggested starting place for this challenge is to create a model 
that first incorporates all the columns from `fct_command_statuses` along with an additional column of type `ARRAY` which holds the sensor reading(s)
produced by the targeted device (as a `STRUCT`) in the 15 seconds that followed the `Command` for the command in question. You'll need to use
timing because the readings produced by a command do not supply a `command_uuid` which tie them back to a command.

Often when we start aggregating data in this way we find that we have even more questions. This case is no exception. Here are a couple of examples
of questions that may arise:
* Some of these commands are less than 15 seconds apart, what implications does this have for these results?
* How many of the commands with `is_hub_success` as `True` were followed (within 15 seconds) by a reading that matches the command's desired state?

Can you update your model with additional columns or other changes to address these questions? Are there questions you would like to ask?

**Hints:**
* Follow the pattern you've seen in Parts One and Two
* For timing it's recommended to use `raw_sync_events.DateCreated` which you may remember from Part One is renamed to `update_timestamp` 
in the command staging models
* Think about the most effective way to stage the readings you'll need to include in your final model
* As you saw in Part One some commands are not going to have a CommandResult response, or will have a CommandResult indicating failure.
How will this change your approach? 
* The `ItemData` description in `dbt/models/staging/schema.yml` contains invaluable context for the task of matching commands to readings

Finally, if you run into a wall or just need something clarified please reach out with questions. Your questions are a going to be
a big part of working together. The open nature of this final challenge is intentional--we love to ponder and reflect at Dwelo and
we look forward to reflecting on this challenge with you. 

##Submitting Your Work
Once you're ready to submit your work, ensure that you've committed and pushed all the changes to your working branch to github.

Open a PR against your forked repo and invite https://github.com/specktastic as a reviewer.