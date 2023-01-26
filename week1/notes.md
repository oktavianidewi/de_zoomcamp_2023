`sudo chmod 666 /var/run/docker.sock`
to avoid errors: 
```
docker: Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock: Post "http://%2Fvar%2Frun%2Fdocker.sock/v1.24/containers/create": dial unix /var/run/docker.sock: connect: permission denied.
```

https://medium.com/@ywg/data-engineering-zoomcamp-series-my-notes-8fd1f4706716