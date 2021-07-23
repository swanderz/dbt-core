{#

data engineering create table workflow
1. drop corresponding backup & intermediate table
2. if the desired table already exits, rename it to the backup table
3. use create_table_as macro to load results of sql query into the intermediate table
4. rename the intermediate table to the desired table
5. create any necessary indices
6. drop the backup table

if anything goes wrong in the above at any time,
restore things back to how they were


hooks:
  they are called by the macro at either
  - the beginning of the macro (pre-hooks)
  - the end of the macro (post-hooks)
 #}


{% materialization table, default %}

  {# normally the relation name is the name of the .sql or .csv file  #}
  {# however, sometimes you want it to be different  #}
  {# enter the ALIAS which lets you have the relations have  #}
  {# a different name in the db than their file name  #}
  {%- set identifier = model['alias'] -%}


  {# normal data engine #}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}


  {# does the relation exist?
     if so, we're going to need to drop it #}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {# api.Relation.create
     this will make a Relation object which is a dataclass(?)
     that defines the three-part name and alias.
     we'll log it to see what happens
  #}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}
  {{ log('xxx target_relation: ' ~ target_relation , info=True) }}

  {# same thing here but with a temp table that we'll rename to our target table #}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema,
                                                      database=database,
                                                      type='table') -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = adapter.get_relation(identifier=tmp_identifier, 
                                                                   schema=schema,
                                                                   database=database) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation_type = 'table' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema,
                                                database=database,
                                                type=backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = adapter.get_relation(identifier=backup_identifier,
                                                             schema=schema,
                                                             database=database) -%}


  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, intermediate_relation, sql) }}
  {%- endcall %}

  -- cleanup
  {% if old_relation is not none %}
      {{ adapter.rename_relation(target_relation, backup_relation) }}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
