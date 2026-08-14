module object_storage_tests {
  import object_storage.* from "./object_storage"

  run overwriteTest =
    init
      .then(putObject("key", "first", Overwrite))
      .then(putObject("key", "second", Overwrite))
      .expect(getObject("key") == { data: "second", etag: 1 })

  run createNewObjectTest =
    init
      .then(putObject("key", "data", Create))
      .expect(getObject("key") == { data: "data", etag: 0 })

  run createExistingObjectTest =
    init
      .then(putObject("key", "first", Overwrite))
      .then(putObject("key", "second", Create).fail())

  run updateMatchingETagTest =
    init
      .then(putObject("key", "first", Overwrite))
      .then(putObject("key", "second", Update(0)))
      .expect(getObject("key") == { data: "second", etag: 1 })

  run updateMismatchedETagTest =
    init
      .then(putObject("key", "first", Overwrite))
      .then(putObject("key", "second", Update(42)).fail())

  run deleteObjectTest =
    init
      .then(putObject("key", "data", Overwrite))
      .then(deleteObject("key"))
      .expect(objectKeys == Set())

  run deleteMissingObjectTest =
    init
      .then(putObject("key", "data", Overwrite))
      .then(deleteObject("missing"))
      .expect(objectKeys == Set("key"))

  run listObjectsTest =
    init
      .then(putObject("first", "one", Overwrite))
      .then(putObject("second", "two", Overwrite))
      .expect(objectKeys == Set("first", "second"))
}
