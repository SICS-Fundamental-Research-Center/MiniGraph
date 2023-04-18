# example for wcc
./bin/graph_partition_exec -t csr_bin -p -n 1 -i inputs/roadNet-CA.csv -sep "," -o inputs/workspace/ -cores 10 -tobin -partitioner vertexcut
./bin/wcc_vc_exec -i inputs/workspace/ -cc 1 -buffer_size 1 -cores 4


# docker
docker run -it --rm -d -p 8022:22 -v /home/liuyang/planar/MiniGraph:/root/planar xiaokezhu/minigraph_env bash