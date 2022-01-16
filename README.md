# Decentralized websites on Peerster network
Adaptation of the Peerster client which allows upload and seamless browsing of static websites in a decentralized manner.
Website files are split into chunks which are content-addressed and stored throughout the network. The DHT holds global info on who holds what chunk. IPNS style pointer records are introduced to allow editing files while keeping them under the same address in the DHT. 

Project for CS-438 (Decentralized Systems Engineering) at EPFL.

## How to run
You can run the program by navigating to the gui folder and running the following command:
```console
go run mod.go start
```

Then open the file located at _gui/web/index.html_ and follow the instructions using the proxy server address found in the log of the go command above:
```console
proxy server is ready to handle requests at '<proxy_address>'
```
