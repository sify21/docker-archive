A Rust library to insecpt a [docker archive](https://github.com/moby/moby/tree/master/image/spec). Inspired by [dive](https://github.com/wagoodman/dive), almost a direct translate from it but without UI.
## TODOs
- [ ] allow usage in async runtime. (currently all related codes need to be enclosed in curly braces, and no `.await` in between, because the returned `Rc` is a non-Send type.)
## Warning
The project is not stable yet, the public api may change, and semver rules may be broken.
