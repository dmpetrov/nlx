def imp(self, url, path, out=None, fname=None, rev=None, **kwargs):
    erepo = {"url": url}
    if rev is not None:
        erepo["rev"] = rev

    return self.imp_url(
        path, out=out, fname=fname, erepo=erepo, frozen=True, **kwargs
    )
