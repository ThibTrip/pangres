# Generating the documentation

1. Use the provided environment (see `environment.yml` at the root of the repo) or have `pangres` and `npdoc_to_md` both installed
2. Use the following command while in the `wiki` folder (where this README file currently is):

```
npdoc-to-md render-folder -source ./templates --destination .
```

The directory `./templates` contains templates with placeholders following the syntax defined in the library [npdoc_to_md](https://github.com/ThibTrip/npdoc_to_md/).