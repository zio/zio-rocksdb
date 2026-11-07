const sidebars = {
  sidebar: [
    {
      type: "category",
      label: "ZIO RocksDB",
      collapsed: false,
      link: { type: "doc", id: "index" },
      items: [
        "serialization",
        "iterators",
        "transactions",
        "write-batch"
      ]
    }
  ]
};

module.exports = sidebars;
