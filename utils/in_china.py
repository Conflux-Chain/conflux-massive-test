def region_in_china(region_id: str) -> bool:
    if not region_id:
        return True
    if region_id.startswith("cn"):
        # aliyun
        return True
    for city in {
        "beijing",
        "shanghai",
        "nanjing",
        "guangzhou",
        "chengdu",
        "chongqing",
        "zhongwei",  # 中卫
        "shenzhen",
    }:
        if city in region_id:
            # tencent cloud
            return True
    return False