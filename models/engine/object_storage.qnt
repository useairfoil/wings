// A strongly consistent object-storage model with S3-style conditional writes.
//
// Import it with a short alias so storage state and operations remain recognizable:
//
//   import object_storage as s3 from "./object_storage"
//
// The key type `k` and stored data type `a` are generic and inferred from the
// importing model. For example, writing a Document under an integer key makes
// `s3::objects` an `int -> s3::StoredObject[Document]` map. Keep models using
// different key or data types in separate .qnt files so Quint can infer each
// import independently.
//
// A typical lifecycle is:
//
//   s3::init
//     .then(s3::putObject(1, document, s3::Create))
//
// Read `s3::getObject(key)` to obtain both `data` and `etag`, then use
// `s3::Update(etag)` for compare-and-set. A failed precondition disables
// the put action, so tests can assert rejection with `.fail()`.
module object_storage {
  // ETags are unique, monotonically increasing integers within this model.
  type StoredObject[a] = { data: a, etag: int }

  // Overwrite always writes. Create requires an absent key. Update implements
  // CAS by requiring the current object's ETag to equal the supplied ETag.
  type PutMode = Overwrite | Create | Update(int)

  // Importing models that initialize or frame storage directly must assign or
  // preserve both state variables. Prefer the public actions where possible.
  var objects: k -> StoredObject[a]
  var nextETag: int

  // Start with empty storage. The first successful write receives ETag 0.
  action init = all {
    objects' = Map(),
    nextETag' = 0,
  }

  def etagMatches(key: k, expected: int): bool =
    if (objects.keys().contains(key)) objects.get(key).etag == expected else false

  action storeObject(key: k, data: a): bool = all {
    objects' = objects.put(key, { data: data, etag: nextETag }),
    nextETag' = nextETag + 1,
  }

  action putObject(key: k, data: a, mode: PutMode): bool =
    match mode {
      | Overwrite => storeObject(key, data)
      | Create => all {
          not(objects.keys().contains(key)),
          storeObject(key, data),
        }
      | Update(expected) => all {
          etagMatches(key, expected),
          storeObject(key, data),
        }
    }

  // Return data and ETag. Like a map lookup, reading a missing key is undefined;
  // callers should check `objects.keys().contains(key)` when absence is possible.
  def getObject(key: k): StoredObject[a] = objects.get(key)

  // Delete is idempotent: deleting a missing key succeeds without changing the
  // object map or consuming an ETag.
  action deleteObject(key: k): bool = all {
    objects' = objects.keys().exclude(Set(key)).mapBy(k => objects.get(k)),
    nextETag' = nextETag,
  }

  // Return all keys. Models that attach path or prefix semantics to their key
  // type can filter this set without coupling storage to one key encoding.
  val objectKeys: Set[k] = objects.keys()
}
