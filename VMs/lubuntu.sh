qemu-system-x86_64 \
-hda lubuntu.qcow2 \
-m 512 \
-enable-kvm \
-no-reboot \
-display none \
-net nic \
-serial mon:stdio \
-net user,hostfwd=tcp::2222-:22,hostfwd=tcp::22280-:80
