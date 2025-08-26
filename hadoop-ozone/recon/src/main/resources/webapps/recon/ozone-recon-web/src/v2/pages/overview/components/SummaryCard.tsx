import React from 'react';
import OverviewSimpleCard from '@/v2/components/overviewCard/overviewSimpleCard';
import { Col } from 'antd';
import ErrorMessage from '@/v2/components/errors/errorMessage';

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
      <Col flex="1 0 20%">
        <OverviewSimpleCard
          title='Volumes'
          icon='inbox'
          loading={loading}
          data={volumes}
          linkToUrl='/Volumes' />
      </Col>
      <Col flex="1 0 20%">
        <OverviewSimpleCard
          title='Buckets'
          icon='folder-open'
          loading={loading}
          data={buckets}
          linkToUrl='/Buckets' />
      </Col>
      <Col flex="1 0 20%">
        <OverviewSimpleCard
          title='Keys'
          icon='file-text'
          loading={loading}
          data={keys} />
      </Col>
      <Col flex="1 0 20%">
        <OverviewSimpleCard
          title='Pipelines'
          icon='deployment-unit'
          loading={loading}
          data={pipelines}
          linkToUrl='/Pipelines' />
      </Col>
      <Col flex="1 0 20%">
        <OverviewSimpleCard
          title='Deleted Containers'
          icon='delete'
          loading={loading}
          data={deletedContainers} />
      </Col>
    </>
  )
}

export default SummaryCard;
