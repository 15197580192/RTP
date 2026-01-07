 # Neo4j 3.5+ 版本中，PERIODIC COMMIT 已经不是必须的（Neo4j 会自动优化批量导入）       
    load csv with headers       
    from 'file:///comment_0_0.csv' as csvline
    FIELDTERMINATOR '|'  
    create (   
    node:Comment{          
        id: csvline.id,
        creationDate: csvline.creationDate,
        locationIP: csvline.locationIP,
        browserUsed: csvline.browserUsed,
        content: csvline.content,
        length: csvline.length
        }
    );

    load csv with headers
    from 'file:///forum_0_0.csv' as csvline
    FIELDTERMINATOR '|'  
    create (
        node:Forum{
        id: csvline.id,
        title: csvline.title,
        creationDate: csvline.creationDate
        }
    );


    load csv with headers
    from 'file:///organisation_0_0.csv' as csvline
    FIELDTERMINATOR '|' 
    create (
    node:Organisation{
        id: csvline.id,
    type:csvline.type,
    name:csvline.name,
        url: csvline.url
        }
    );


    load csv with headers
    from 'file:///person_0_0.csv' as csvline
    FIELDTERMINATOR '|' 
    create (
    node:Person{
        id: csvline.id,
    firstName: csvline.firstName,
    lastName: csvline.lastName,
    gender: csvline.gender,
    birthday: csvline.birthday,
    creationDate: csvline.creationDate,
        locationIP: csvline.locationIP,
    browserUsed: csvline.browserUsed
        }
    );


    load csv with headers
    from 'file:///place_0_0.csv' as csvline
    FIELDTERMINATOR '|' 
    create (
    node:Place{
        id: csvline.id,
    name: csvline.name,
        url: csvline.url,
    type: csvline.type
        }
    );

    load csv with headers
    from 'file:///post_0_0.csv' as csvline
    FIELDTERMINATOR '|' 
    create (
    node:Post{
        id: csvline.id,
    imageFile: csvline.imageFile,
    creationDate: csvline.creationDate,
        locationIP: csvline.locationIP,
    browserUsed: csvline.browserUsed,
    language: csvline.language,
    content: csvline.content,
    length: csvline.length
        }
    );

    load csv with headers
    from 'file:///tag_0_0.csv' as csvline
    FIELDTERMINATOR '|' 
    create (
    node:Tag{
        id: csvline.id,
    name: csvline.name,
        url: csvline.url
        }
    );

    load csv with headers
    from 'file:///tagclass_0_0.csv' as csvline
    FIELDTERMINATOR '|' 
    create (
    node:Tagclass{
        id: csvline.id,
    name: csvline.name,
        url: csvline.url
        }
    );

 create index on :Comment(id);
 create index on :Forum(id);
 create index on :Organisation(id);
 create index on :Person(id);
 create index on :Place(id);
 create index on :Post(id);
 create index on :Tag(id);
 create index on :Tagclass(id);


       LOAD CSV WITH HEADERS 
       FROM "file:///comment_hasCreator_person_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (comment:Comment {id: csvline.Comment_id}), (person:Person {id: csvline.Person_id})
       CREATE (comment)-[:hasCreator]->(person);



       LOAD CSV WITH HEADERS 
       FROM "file:///comment_hasTag_tag_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (comment:Comment {id: csvline.Comment_id}), (tag:Tag {id: csvline.Tag_id})
       CREATE (comment)-[:hasTag]->(tag);



       LOAD CSV WITH HEADERS 
       FROM "file:///comment_isLocatedIn_place_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (comment:Comment {id: csvline.Comment_id}), (place:Place {id: csvline.Place_id})
       CREATE (comment)-[:isLocatedIn]->(place);



                     LOAD CSV WITH HEADERS 
                     FROM "file:///forum_containerOf_post_0_0.csv" AS csvline
                     FIELDTERMINATOR '|'
                     MATCH (forum:Forum {id: csvline.Forum_id}), (post:Post {id: csvline.Post_id})
                     CREATE (forum)-[:containerOf]->(post);


LOAD CSV WITH HEADERS 
FROM "file:///forum_hasTag_tag_0_0.csv" AS csvline
FIELDTERMINATOR '|'
MATCH (forum:Forum {id: csvline.Forum_id}), (tag:Tag {id: csvline.Tag_id})
CREATE (forum)-[r:hasTag]->(tag);


       LOAD CSV WITH HEADERS 
       FROM "file:///organisation_isLocatedIn_place_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (organisation:Organisation {id: csvline.Organisation_id}), (place:Place {id: csvline.Place_id})
       CREATE (organisation)-[r:isLocatedIn]->(place);



       LOAD CSV WITH HEADERS 
       FROM "file:///person_isLocatedIn_place_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (person:Person {id: csvline.Person_id}), (place:Place {id: csvline.Place_id})
       CREATE (person)-[r:isLocatedIn]->(place);



       LOAD CSV WITH HEADERS 
       FROM "file:///person_studyAt_organisation_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (person:Person {id: csvline.Person_id}), (organisation:Organisation {id: csvline.Organisation_id})
       MERGE (person)-[r:studyAt]->(organisation)
       ON CREATE SET r.classYear = csvline.classYear;



       LOAD CSV WITH HEADERS 
       FROM "file:///place_isPartOf_place_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (place1:Place {id: csvline.Place_id1}), (place2:Place {id: csvline.Place_id2})
       CREATE (place1)-[r:isPartOf]->(place2);



       LOAD CSV WITH HEADERS 
       FROM "file:///post_hasCreator_person_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (post:Post {id: csvline.Post_id}), (person:Person {id: csvline.Person_id})
       CREATE (post)-[r:hasCreator]->(person);



       LOAD CSV WITH HEADERS 
       FROM "file:///post_hasTag_tag_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (post:Post {id: csvline.Post_id}), (tag:Tag {id: csvline.Tag_id})
       CREATE (post)-[r:hasTag]->(tag);



       LOAD CSV WITH HEADERS 
       FROM "file:///post_isLocatedIn_place_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (post:Post {id: csvline.Post_id}), (place:Place {id: csvline.Place_id})
       CREATE (post)-[r:isLocatedIn]->(place);



       LOAD CSV WITH HEADERS 
       FROM "file:///tag_hasType_tagclass_0_0.csv" AS csvline
       FIELDTERMINATOR '|'
       MATCH (tag:Tag {id: csvline.Tag_id}), (tagclass:Tagclass {id: csvline.TagClass_id})
       CREATE (tag)-[r:hasType]->(tagclass);
