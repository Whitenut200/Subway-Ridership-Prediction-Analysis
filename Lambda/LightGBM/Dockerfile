FROM public.ecr.aws/lambda/python:3.9

# 1. 시스템 도구 설치 (CMake, gcc, libomp 등)
RUN yum -y groupinstall "Development Tools" && \
    yum -y install wget tar libomp libgomp libffi-devel && \
    cd /opt && \
    wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.tar.gz && \
    tar -zxvf cmake-3.26.4-linux-x86_64.tar.gz && \
    ln -s /opt/cmake-3.26.4-linux-x86_64/bin/* /usr/local/bin/

# 2. requirements.txt 복사 및 설치
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# 3. lambda 핸들러 파일 복사
COPY predict_lightgbm.py .

# 4. 진입점 설정 (모듈명.함수명)
CMD ["predict_lightgbm.lambda_handler"]
