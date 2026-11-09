#pragma once

#include <librdkafka/rdkafkacpp.h>


class ExampleDeliveryReportCb: public RdKafka::DeliveryReportCb {
    public:
    void dr_cb(RdKafka::Message & message) override;
    
      
};