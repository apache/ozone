type GlobalStorage = {
  totalUsedSpace: number;
  totalFreeSpace: number;
  totalCapacity: number;
};

type GlobalNamespace = {
  totalUsedSpace: number;
  totalKeys: number;
};

type UsedSpaceBreakdown = {
  openKeysBytes: number;
  committedBytes: number;
  containerPreAllocated: number;
  deletionPendingBytes: {
    total: number;
    byStage: {
      DN: {
        pendingBytes: number;
      };
      SCM: {
        pendingBytes: number;
      };
      OM: {
        pendingKeyBytes: number;
        pendingBytes: number;
        pendingDirectoryBytes: number;
      };
    };
  };
};

type DataNodeUsage = {
  uuid: string;
  capacity: number;
  used: number;
  remaining: number;
  committed: number;
  pendingDeletion: number;
};

type UtilizationResponse = {
  globalStorage: GlobalStorage;
  globalNamespace: GlobalNamespace;
  usedSpaceBreakdown: UsedSpaceBreakdown;
  dataNodeUsage: DataNodeUsage[];
};

type Segment = {
  value: number;
  color: string;
  label: string | React.ReactNode;
};
