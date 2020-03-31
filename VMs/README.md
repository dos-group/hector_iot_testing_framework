## QEMU information

### qcow2 file creation
+ `qemu-img create -f qcow2 xxx.qcow2`
+ `qemu-system-x86_64 -boot d -cdrom image.iso -m 512 -hda xxx.qcow2`
+ install Linux OS from image.iso (eg. lubuntu, arch, ...) to the previously create xxx.qcow2 image

### Example QEMU starting arguments for testing
+ Raspberry Pi image
    + `qemu-system-arm -M versatilepb -kernel pathTo/kernel-qemu-4.14.79-stretch -append "root=/dev/sda2 panic=1 rootfstype=ext4 rw" -hda pathTo/raspbian-stretch-lite.qcow -dtb pathTo/QEMU/versatile-pb.dtb -cpu arm1176 -m 256 -no-reboot -machine versatilepb -net nic -net user,hostfwd=tcp::2222-:22,hostfwd=tcp::22280-:80`
+ Lubuntu image (kvm needs root)
    + `sudo qemu-system-x86_64 -hda pathTo/lubuntu.qcow2 -m 512 -enable-kvm -no-reboot -net nic -serial mon:stdio -net user,hostfwd=tcp::2222-:22,hostfwd=tcp::22280-:80`

### Preparations for usage
+ SSH
    + Possible package: `openssh-client`
    +  If it won't start on system boot, a crontab can be used
        + `sudo crontab -e`
        + Enter following line `@reboot service ssh restart`
    + Copy ssh key from Host to VM ex. `ssh-copy-id -p 2222 -i pathTo/id_rsa lubuntu@127.0.0.1`
+ Java jre 8
    + Possible package: `openjdk-8-jre-headless`
+ Flink/Kafka/Flink for edge
    +  Copy directories to QEMU image
        + `scp -p 2222 -r Flink/ user@127.0.0.1:/home/user/`
        + `scp -p 2222 -r Kafka/ user@127.0.0.1:/home/user/`
    + JVM may have some problems starting correctly with less than 512MB RAM
        + Changes in Flink/conf/flink-conf.yaml
            + taskmanager.heap.size: 128m
            + taskmanager.network.memory.min: 32mb
        + Changes in Flink/bin/taskmanager.sh
            + TM_MAX_OFFHEAP_SIZE="200M"

### Useful links
+ https://camp.isaax.io/en/isaax-examples/run-a-virtualized-image-of-raspberry-pi-in-qemu
