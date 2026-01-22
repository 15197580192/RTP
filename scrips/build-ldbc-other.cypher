USING PERIODIC COMMIT 10000       
load csv with headers       
from 'file:///comment_0_0_new.csv' as csvline
FIELDTERMINATOR '|'  
merge (   
node:Comment{          
    id: csvline.id,
    creationDate: csvline.creationDate,
    locationIP: csvline.locationIP,
    browserUsed: csvline.browserUsed,
    content: csvline.content,
    length: csvline.length
    }
);

USING PERIODIC COMMIT 10000       
load csv with headers
from 'file:///forum_0_0.csv' as csvline
FIELDTERMINATOR '|'  
merge (
    node:Forum{
    id: csvline.id,
    title: csvline.title,
    creationDate: csvline.creationDate
    }
);


USING PERIODIC COMMIT 10000       
load csv with headers
from 'file:///organisation_0_0.csv' as csvline
FIELDTERMINATOR '|' 
merge (
node:Organisation{
    id: csvline.id,
type:csvline.type,
name:csvline.name,
    url: csvline.url
    }
);


USING PERIODIC COMMIT 10000       
load csv with headers
from 'file:///person_0_0.csv' as csvline
FIELDTERMINATOR '|' 
merge (
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


USING PERIODIC COMMIT 10000       
load csv with headers
from 'file:///place_0_0.csv' as csvline
FIELDTERMINATOR '|' 
merge (
node:Place{
    id: csvline.id,
name: csvline.name,
    url: csvline.url,
type: csvline.type
    }
);

USING PERIODIC COMMIT 10000       
load csv with headers
from 'file:///post_0_0_new.csv' as csvline
FIELDTERMINATOR '|' 
merge (
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

USING PERIODIC COMMIT 10000       
load csv with headers
from 'file:///tag_0_0.csv' as csvline
FIELDTERMINATOR '|' 
merge (
node:Tag{
    id: csvline.id,
name: csvline.name,
    url: csvline.url
    }
);

USING PERIODIC COMMIT 10000       
load csv with headers
from 'file:///tagclass_0_0.csv' as csvline
FIELDTERMINATOR '|' 
merge (
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

  CALL db.indexes();

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///comment_hasCreator_person_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (comment:Comment {id: csvline.`Comment.id`}), (person:Person {id: csvline.`Person.id`})
MERGE (comment)-[:hasCreator]->(person);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///comment_hasTag_tag_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (comment:Comment {id: csvline.`Comment.id`}), (tag:Tag {id: csvline.`Tag.id`})
MERGE (comment)-[:hasTag]->(tag);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///comment_isLocatedIn_place_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (comment:Comment {id: csvline.`Comment.id`}), (place:Place {id: csvline.`Place.id`})
MERGE (comment)-[:isLocatedIn]->(place);

USING PERIODIC COMMIT 10000       
load csv
FROM "file:///comment_replyOf_comment_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
WITH csvline
WHERE csvline[0] <> 'Comment.id'
MATCH (comment1:Comment {id: csvline[0]}), (comment2:Comment {id: csvline[1]})
MERGE (comment1)-[:replyOf]->(comment2);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///comment_replyOf_post_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (comment:Comment {id: csvline.`Comment.id`}), (post:Post {id: csvline.`Post.id`})
MERGE (comment)-[:replyOf]->(post);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///forum_containerOf_post_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (forum:Forum {id: csvline.`Forum.id`}), (post:Post {id: csvline.`Post.id`})
MERGE (forum)-[:containerOf]->(post);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///forum_hasMember_person_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (forum:Forum {id: csvline.`Forum.id`}), (person:Person {id: csvline.`Person.id`})
MERGE (forum)-[r:hasMember]->(person)
ON CREATE SET r.joinDate = csvline.joinDate;

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///forum_hasModerator_person_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (forum:Forum {id: csvline.`Forum.id`}), (person:Person {id: csvline.`Person.id`})
MERGE (forum)-[r:hasModerator]->(person);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///forum_hasTag_tag_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (forum:Forum {id: csvline.`Forum.id`}), (tag:Tag {id: csvline.`Tag.id`})
MERGE (forum)-[r:hasTag]->(tag);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///organisation_isLocatedIn_place_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (organisation:Organisation {id: csvline.`Organisation.id`}), (place:Place {id: csvline.`Place.id`})
MERGE (organisation)-[r:isLocatedIn]->(place);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///person_hasInterest_tag_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (person:Person {id: csvline.`Person.id`}), (tag:Tag {id: csvline.`Tag.id`})
MERGE (person)-[r:hasInterest]->(tag);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///person_isLocatedIn_place_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (person:Person {id: csvline.`Person.id`}), (place:Place {id: csvline.`Place.id`})
MERGE (person)-[r:isLocatedIn]->(place);

USING PERIODIC COMMIT 10000       
load csv
FROM "file:///person_knows_person_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
WITH csvline
WHERE csvline[0] <> 'Person.id'
MATCH (person1:Person {id: csvline[0]}), (person2:Person {id: csvline[1]})
MERGE (person1)-[r:knows]->(person2)
ON CREATE SET r.creationDate = csvline[2]
MERGE (person2)-[r1:knows]->(person1)
ON CREATE SET r1.creationDate = csvline[2];

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///person_likes_comment_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (person:Person {id: csvline.`Person.id`}), (comment:Comment {id: csvline.`Comment.id`})
MERGE (person)-[r:likes]->(comment)
ON CREATE SET r.creationDate = csvline.creationDate;

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///person_likes_post_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (person:Person {id: csvline.`Person.id`}), (post:Post {id: csvline.`Post.id`})
MERGE (person)-[r:likes]->(post)
ON CREATE SET r.creationDate = csvline.creationDate;

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///person_studyAt_organisation_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (person:Person {id: csvline.`Person.id`}), (organisation:Organisation {id: csvline.`Organisation.id`})
MERGE (person)-[r:studyAt]->(organisation)
ON CREATE SET r.classYear = csvline.classYear;

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///person_workAt_organisation_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (person:Person {id: csvline.`Person.id`}), (organisation:Organisation {id: csvline.`Organisation.id`})
MERGE (person)-[r:workAt]->(organisation)
ON CREATE SET r.workFrom = csvline.workFrom;


USING PERIODIC COMMIT 10000       
load csv
FROM "file:///place_isPartOf_place_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
WITH csvline
WHERE csvline[0] <> 'Place.id'
MATCH (place1:Place {id: csvline[0]}), (place2:Place {id: csvline[1]})
MERGE (place1)-[r:isPartOf]->(place2);

USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///post_hasCreator_person_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (post:Post {id: csvline.`Post.id`}), (person:Person {id: csvline.`Person.id`})
MERGE (post)-[r:hasCreator]->(person);


USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///post_hasTag_tag_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (post:Post {id: csvline.`Post.id`}), (tag:Tag {id: csvline.`Tag.id`})
MERGE (post)-[r:hasTag]->(tag);


USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///post_isLocatedIn_place_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (post:Post {id: csvline.`Post.id`}), (place:Place {id: csvline.`Place.id`})
MERGE (post)-[r:isLocatedIn]->(place);


USING PERIODIC COMMIT 10000       
load csv with headers 
FROM "file:///tag_hasType_tagclass_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
MATCH (tag:Tag {id: csvline.`Tag.id`}), (tagclass:Tagclass {id: csvline.`TagClass.id`})
MERGE (tag)-[r:hasType]->(tagclass);


USING PERIODIC COMMIT 10000       
load csv
FROM "file:///tagclass_isSubclassOf_tagclass_0_0.csv" AS csvline
FIELDTERMINATOR '|' 
WITH csvline
WHERE csvline[0] <> 'Tagclass.id'
MATCH (tagclass1:Tagclass {id: csvline[0]}), (tagclass2:Tagclass {id: csvline[1]})
MERGE (tagclass1)-[r:isSubclassOf]->(tagclass2);
