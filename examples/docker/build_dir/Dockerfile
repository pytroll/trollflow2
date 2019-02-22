FROM continuumio/miniconda3:latest

RUN apt-get update && apt-get -y --no-install-recommends install \
    gosu

RUN conda config --add channels conda-forge && \
    conda update conda && \
    conda install satpy \
    pyresample \
    pykdtree \
    trollimage \
    pycoast \
    pyorbital \
    rasterio \
    gdal \
    h5py \
    ipython \
    pyinotify \
    pyzmq \
    h5netcdf \
    netifaces \
    meld3 \
    scp \
    paramiko \
    dpath \
    toolz \
    configobj && \
    conda clean -a -y
ADD requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt && \
    rm -r /tmp/requirements.txt &&  \
    cd /tmp/ && \
    git clone https://github.com/Supervisor/supervisor.git && \
    cd supervisor && python setup.py install && \
    cd /tmp/ && rm -rf /tmp/supervisor/ && \
    git clone https://github.com/Supervisor/superlance.git && \
    cd superlance && python setup.py install && \
    cd /tmp/ && rm -rf /tmp/superlance/
    git clone https://github.com/pnuu/trollflow2.git && \
    cd trollflow2 && git checkout feature-docker-example && python setup.py install && \
    cd /tmp/ && rm -rf /tmp/trollflow2/
    #python -c 'from pyspectral.utils import download_luts; \
    #  from pyspectral.utils import logging_on; import logging; \
    #  logging_on(logging.DEBUG); download_luts()' && \
    #python -c 'from pyspectral.utils import download_rsr; \
    #  from pyspectral.utils import logging_on; import logging; \
    #  logging_on(logging.DEBUG); download_rsr()' && \
    #mkdir -p /home/user/.local/share && \
    #mv /root/.local/share/pyspectral /home/user/.local/share/
    
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

#COPY trollflow2 /tmp/trollflow2
#RUN cd /tmp/trollflow2 && python setup.py install && \
#     cd /tmp/ && rm -rf /tmp/trollflow2/

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["supervisord", "-c", "/mnt/config/supervisord.conf"]
