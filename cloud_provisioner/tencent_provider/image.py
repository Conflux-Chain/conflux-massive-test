from typing import List
from tencentcloud.cvm.v20170312 import models as cvm_models
from tencentcloud.cvm.v20170312.cvm_client import CvmClient
from cloud_provisioner.create_instances.types import ImageInfo


def as_image_info(rep: cvm_models.Image) -> ImageInfo:
    assert isinstance(rep.ImageId, str)
    assert isinstance(rep.ImageName, str)
    return ImageInfo(image_id=rep.ImageId, image_name=rep.ImageName)


def get_images_in_region(client: CvmClient, image_name: str) -> List[ImageInfo]:
    result: List[ImageInfo] = []
    offset = 0
    limit = 100

    filter_name = cvm_models.Filter()
    filter_name.Name = "image-name"
    filter_name.Values = [image_name]

    while True:
        req = cvm_models.DescribeImagesRequest()
        req.Filters = [filter_name]
        req.Offset = offset
        req.Limit = limit

        resp = client.DescribeImages(req)
        if resp.ImageSet:
            result.extend([as_image_info(img) for img in resp.ImageSet])

        if resp.TotalCount is None or resp.TotalCount <= offset + limit:
            break
        offset += limit

    return result
