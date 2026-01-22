import csv

csvEdgesFile = {'comment_hasCreator_person_0_0.csv': ['comment_0_0.csv','person_0_0.csv'],
                'comment_hasTag_tag_0_0.csv': ['comment_0_0.csv','tag_0_0.csv'], 
                'comment_isLocatedIn_place_0_0.csv': ['comment_0_0.csv','place_0_0.csv'],  
                'comment_replyOf_comment_0_0.csv': ['comment_0_0.csv','comment_0_0.csv'],  
                'comment_replyOf_post_0_0.csv': ['comment_0_0.csv','post_0_0.csv'],  
                'forum_containerOf_post_0_0.csv': ['forum_0_0.csv','post_0_0.csv'],  
                'forum_hasMember_person_0_0.csv': ['forum_0_0.csv','person_0_0.csv'],  
                'forum_hasModerator_person_0_0.csv': ['forum_0_0.csv','person_0_0.csv'], 
                'forum_hasTag_tag_0_0.csv': ['forum_0_0.csv','tag_0_0.csv'], 
                'organisation_isLocatedIn_place_0_0.csv': ['organisation_0_0.csv','place_0_0.csv'], 
                'person_hasInterest_tag_0_0.csv': ['person_0_0.csv','tag_0_0.csv'], 
                'person_isLocatedIn_place_0_0.csv': ['person_0_0.csv','place_0_0.csv'], 
                'person_knows_person_0_0.csv': ['person_0_0.csv','person_0_0.csv'], 
                'person_likes_comment_0_0.csv': ['person_0_0.csv','comment_0_0.csv'], 
                'person_likes_post_0_0.csv': ['person_0_0.csv','post_0_0.csv'], 
                'person_studyAt_organisation_0_0.csv': ['person_0_0.csv','organisation_0_0.csv'], 
                'person_workAt_organisation_0_0.csv': ['person_0_0.csv','organisation_0_0.csv'], 
                'place_isPartOf_place_0_0.csv': ['place_0_0.csv','place_0_0.csv'], 
                'post_hasCreator_person_0_0.csv': ['post_0_0.csv','person_0_0.csv'], 
                'post_hasTag_tag_0_0.csv': ['post_0_0.csv','tag_0_0.csv'],  
                'post_isLocatedIn_place_0_0.csv': ['post_0_0.csv','place_0_0.csv'], 
                'tag_hasType_tagclass_0_0.csv': ['tag_0_0.csv','tagclass_0_0.csv'], 
                'tagclass_isSubclassOf_tagclass_0_0.csv': ['tagclass_0_0.csv','tagclass_0_0.csv']}

inPath = '/data/wangliang/partition_8_26G_csv/HDRF/'
num = 0
for i in range(8):
    for j in csvEdgesFile:
        with open(inPath+str(i)+'/'+j, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)
            for raw in csv_reader:
                num += 1

print(num)