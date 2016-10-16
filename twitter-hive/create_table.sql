set hive.support.sql11.reserved.keywords=false;

drop table if exists twitter_data;
create external table twitter_data (
  createddate string,
  geolocation string,
  tweetmessage string,
  user struct <
    geoenabled: boolean,
    id: int,
    name: string,
    screenname: string,
    userlocation: string>
)
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/user/ec2-user/twitter';

