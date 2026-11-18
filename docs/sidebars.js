const sidebars = {
  sidebar: [
    "index",
    "getting-started",
    {
      type: "category",
      label: "Reference",
      collapsed: false,
      items: [
        "reference-rocksdb",
        "reference-column-families",
        "serialization",
        "iterators",
        "write-batch",
        "transactions",
        "reference-transaction-api"
      ]
    },
    "tutorial-user-store",
    {
      type: "category",
      label: "How-to Guides",
      collapsed: false,
      items: [
        "how-to-use-column-families",
        "how-to-open-existing-column-families",
        "how-to-store-typed-keys-and-values",
        "how-to-scan-a-prefix",
        "how-to-update-keys-atomically",
        "how-to-test-with-a-temporary-database",
        "how-to-retry-on-lock-timeout",
        "how-to-choose-between-rocksdb-transactiondb-and-writebatch"
      ]
    }
  ]
};

module.exports = sidebars;
