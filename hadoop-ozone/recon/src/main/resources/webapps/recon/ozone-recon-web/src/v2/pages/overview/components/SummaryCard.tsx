import React from 'react';
import OverviewSimpleCard from '@/v2/components/overviewCard/overviewSimpleCard';
import { Col } from 'antd';
import ErrorMessage from '@/v2/components/errors/errorCard';

type SummaryCardProps = {
  loading: boolean;
  error: string | null;
  volumes: number;
  buckets: number;
  keys: number;
  pipelines: number;
  deletedContainers: number;
}

const SummaryCard: React.FC<SummaryCardProps> = ({
  loading,
  error,
  volumes,
  buckets,
  keys,
  pipelines,
  deletedContainers
}) => {

  if (error) {
    return (
      <Col flex="1 0" className="summary-error-msg">
        <ErrorMessage />
      </Col>
    )
  }

  return (
    <>
      
    </>
  )
}

export default SummaryCard;
