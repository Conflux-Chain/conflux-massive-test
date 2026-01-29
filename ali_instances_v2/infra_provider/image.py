from dataclasses import dataclass
from typing import List
from alibabacloud_ecs20140526.models import DescribeImagesRequest, DescribeImagesResponseBodyImagesImage
from alibabacloud_ecs20140526.client import Client as EcsClient

from ali_instances_v2.client_factory import ClientFactory


@dataclass
class ImageInfo:
    image_id: str
    image_name: str
    
    @classmethod
    def from_api_response(cls, rep: DescribeImagesResponseBodyImagesImage):
        assert type(rep.image_id) is str
        assert type(rep.image_name) is str
        
        return ImageInfo(image_id=rep.image_id, image_name=rep.image_name)



def get_images_in_region(c: ClientFactory, region_id: str, image_name: str) -> List[ImageInfo]:
    client = c.build(region_id)
    
    result = []
    
    page_number = 1
    while True:
        rep = client.describe_images(DescribeImagesRequest(region_id=region_id, image_name=image_name, image_owner_alias="self", page_number=page_number, page_size=50))

        result.extend([ImageInfo.from_api_response(vpc) for vpc in rep.body.images.image])
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
    
    return result