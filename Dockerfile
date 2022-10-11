FROM ubuntu:18.04

# DO NOT ADD AS ENV:
#   debconf noninteractive
#          This is the anti-frontend. It never interacts with you  at  all,
#          and  makes  the  default  answers  be used for all questions. It
#          might mail error messages to root, but that's it;  otherwise  it
#          is  completely  silent  and  unobtrusive, a perfect frontend for
#          automatic installs. If you are using this front-end, and require
#          non-default  answers  to questions, you will need to preseed the
#          debconf database; see the section below  on  Unattended  Package
#          Installation for more details.

RUN apt-get update -y && apt-get install -y git curl wget unzip software-properties-common

SHELL ["/bin/bash", "-c"]

RUN git clone https://github.com/tfutils/tfenv /tfenv  \
    && ln -s /tfenv/bin/* /usr/local/bin \
    && tfenv install 1.3.0 && tfenv use 1.3.0

RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections  \
    && curl https://raw.githubusercontent.com/creationix/nvm/master/install.sh | bash  \
    && . $HOME/.nvm/nvm.sh \
    && nvm install --lts \
    && nvm use --lts \
    && npm install --global cdktf-cli@latest

RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get install -y python3.9 python3-pip python3.9-distutils && ln -s /usr/bin/python3.9 /usr/bin/python

RUN python -m pip install -U pip && python -m pip install -U "git+https://github.com/stikkireddy/brickflow.git" \
    && python -m pip install -U "brickflow[deploy] @ git+https://github.com/stikkireddy/brickflow.git" \
    && python -m pip install -U cdktf

CMD ["/bin/bash"]
