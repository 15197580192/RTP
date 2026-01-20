# LDBC-SNB数据格式：CSV-COMPOSIT-LONGDATETIME

# 关闭服务，离线导入！
# bin/neo4j stop

# 删除原库
mv data/databases/ data/databases-back/
mv data/transactions/ data/transactions-back/

# 导入数据
bin/neo4j-admin database import full \
    --nodes=Comment=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Comment.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/comment_0_0.csv \
    --nodes=Forum=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Forum.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/forum_0_0.csv \
    --nodes=Organisation=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/static/Organisation.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/static/organisation_0_0.csv \
    --nodes=Person=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Person.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/person_0_0.csv \
    --nodes=Place=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/static/Place.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/static/place_0_0.csv \
    --nodes=Post=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Post.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/post_0_0.csv \
    --nodes=TagClass=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/static/TagClass.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/static/tagclass_0_0.csv \
    --nodes=Tag=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/static/Tag.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/static/tag_0_0.csv \
    --relationships=hasCreator=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Comment_hasCreator_Person.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/comment_hasCreator_person_0_0.csv \
    --relationships=hasCreator=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Post_hasCreator_Person.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/post_hasCreator_person_0_0.csv \
    --relationships=isLocatedIn=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Comment_isLocatedIn_Country.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/comment_isLocatedIn_place_0_0.csv \
    --relationships=replyOf=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Comment_replyOf_Comment.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/comment_replyOf_comment_0_0.csv \
    --relationships=replyOf=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Comment_replyOf_Post.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/comment_replyOf_post_0_0.csv \
    --relationships=containerOf=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Forum_containerOf_Post.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/forum_containerOf_post_0_0.csv \
    --relationships=hasMember=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Forum_hasMember_Person.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/forum_hasMember_person_0_0.csv \
    --relationships=hasModerator=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Forum_hasModerator_Person.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/forum_hasModerator_person_0_0.csv \
    --relationships=hasTag=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Forum_hasTag_Tag.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/forum_hasTag_tag_0_0.csv \
    --relationships=hasInterest=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Person_hasInterest_Tag.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/person_hasInterest_tag_0_0.csv \
    --relationships=isLocatedIn=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Person_isLocatedIn_City.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/person_isLocatedIn_place_0_0.csv \
    --relationships=knows=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Person_knows_Person.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/person_knows_person_0_0.csv \
    --relationships=likes=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Person_likes_Comment.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/person_likes_comment_0_0.csv \
    --relationships=likes=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Person_likes_Post.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/person_likes_post_0_0.csv \
    --relationships=isPartOf=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/static/Place_isPartOf_Place.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/static/place_isPartOf_place_0_0.csv \
    --relationships=hasTag=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Comment_hasTag_Tag.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/comment_hasTag_tag_0_0.csv \
    --relationships=hasTag=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Post_hasTag_Tag.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/post_hasTag_tag_0_0.csv \
    --relationships=isLocatedIn=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Post_isLocatedIn_Country.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/post_isLocatedIn_place_0_0.csv \
    --relationships=isSubclassOf=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/static/TagClass_isSubclassOf_TagClass.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/static/tagclass_isSubclassOf_tagclass_0_0.csv \
    --relationships=hasType=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/static/Tag_hasType_TagClass.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/static/tag_hasType_tagclass_0_0.csv \
    --relationships=studyAt=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Person_studyAt_University.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/person_studyAt_organisation_0_0.csv \
    --relationships=workAt=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/dynamic/Person_workAt_Company.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/dynamic/person_workAt_organisation_0_0.csv \
    --relationships=isLocatedIn=import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/headers/static/Organisation_isLocatedIn_Place.csv,import/snb-sf0.1/social_network-csv_composite-longdateformatter-sf0.1/static/organisation_isLocatedIn_place_0_0.csv \
    --trim-strings=true --delimiter="|"

# 修改密码
# bin/neo4j start
# bin/cypher-shell -u neo4j -p neo4j
# 12345678
# :exit
