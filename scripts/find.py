for (const coll of ["data_xml","data_yaml","data_json","data_tab","data_csv"]) {
  const results = db[coll].find({
    $or: [
      { first_name: /RAMIL/i, last_name: /BUROV/i },
      { PassengerFirstName: /RAMIL/i, PassengerLastName: /BUROV/i },
      { "name.first": /RAMIL/i, "name.last": /BUROV/i }
    ]
  }).limit(3).toArray();
  if (results.length > 0) {
    print(`\n--- ${coll} ---`);
    printjson(results);
  }
}
