FROM docker.io/library/alpine:3.20
RUN dd if=/dev/urandom of=/big.bin bs=1M count=24 && sha256sum /big.bin > /big.sha
RUN echo "layer two $(cat /big.sha)" > /two.txt
