# Ember's container image: ember_worker, the sort_benchmark driver, Kiln's
# kiln_meta and kiln_server, and gensort/valsort. build_image.sh stages the
# binaries and the shared libraries they link (all but glibc's) built on
# Ubuntu 22.04, so the base must match.
FROM ubuntu:22.04
COPY bin/ /opt/ember/bin/
COPY lib/ /opt/ember/lib/
COPY gensort/ /opt/gensort/64/
ENV PATH=/opt/ember/bin:/opt/gensort/64:$PATH \
    LD_LIBRARY_PATH=/opt/ember/lib \
    GENSORT_DIR=/opt/gensort
