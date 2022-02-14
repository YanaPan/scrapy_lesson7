# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import scrapy
# from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline
from pymongo import MongoClient
from pymongo import errors


class LeruaPipeline:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongobase = client.plitka14022022

    def process_item(self, item, spider):
        collection = self.mongobase[spider.name]
        try:
            collection.insert_one(item)
        except errors.DuplicateKeyError:
            print(f'плитка с артикулем {item["_id"]} уже есть в базе данных')


class LeruaImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        if item['photos']:
            for img in item['photos']:
                try:
                    yield scrapy.Request(img)
                except Exception as e:
                    print(e)

    def item_completed(self, results, item, info):
        if results:
            item['photos'] = [itm[1] for itm in results if itm[0]]
        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        image_guid = request.url.split('/')[-1]
        return f'full/{item["_id"]}/{image_guid}'
