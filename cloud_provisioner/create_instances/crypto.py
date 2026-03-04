from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import base64
import hashlib

def get_fingerprint_from_key(key_path: str, provider: str) -> str:
    """
    从密钥文件获取指纹
    
    Args:
        key_path: 密钥文件路径
        provider: 云服务商 ('aliyun' 或 'aws')
        hash_type: 哈希类型 ('md5' 或 'sha256')
    
    Returns:
        密钥指纹字符串
    """
    # 读取私钥文件
    with open(key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,  # 如果密钥有密码,在这里提供
            backend=default_backend()
        )
    
    # 获取公钥
    public_key = private_key.public_key()
    
    if provider == 'aliyun':
        # 阿里云：使用 OpenSSH 格式的 base64 部分
        public_key_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.OpenSSH,
            format=serialization.PublicFormat.OpenSSH
        )
        # 提取密钥数据(去掉 "ssh-rsa " 等前缀)
        key_data = base64.b64decode(public_key_bytes.split()[1])
        
        return hashlib.md5(key_data).hexdigest()
        
    elif provider == 'aws':
        # AWS：使用 DER 编码的 SubjectPublicKeyInfo 格式
        key_data = public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        fingerprint = hashlib.md5(key_data).hexdigest()
        # 格式化为 xx:xx:xx:... 格式
        return ':'.join(fingerprint[i:i+2] for i in range(0, len(fingerprint), 2))
    elif provider == 'tencent':
        # 腾讯云：使用 OpenSSH 格式的 base64 部分，输出 OpenSSH 常用 MD5 指纹格式
        public_key_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.OpenSSH,
            format=serialization.PublicFormat.OpenSSH
        )
        key_data = base64.b64decode(public_key_bytes.split()[1])
        fingerprint = hashlib.md5(key_data).hexdigest()
        return ':'.join(fingerprint[i:i+2] for i in range(0, len(fingerprint), 2))
    else:
        raise ValueError(f"不支持的云服务商: {provider}")
    
    
def get_public_key_body(path: str) -> str:
    """
    从私钥文件提取公钥
    
    Args:
        path: 私钥文件路径
        password: 私钥密码(可选)
    
    Returns:
        OpenSSH 格式的公钥字符串
    """
    with open(path, 'rb') as f:
        key_data = f.read()
    
    # 尝试加载私钥(支持 PEM 和 OpenSSH 格式)
    try:
        private_key = serialization.load_pem_private_key(
            key_data, 
            password=None, 
            backend=default_backend()
        )
    except ValueError:
        private_key = serialization.load_ssh_private_key(
            key_data, 
            password=None, 
            backend=default_backend()
        )
    
    # 提取公钥并转换为 OpenSSH 格式
    public_key = private_key.public_key()
    public_key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH
    )
    
    return public_key_bytes.decode('utf-8').strip()

