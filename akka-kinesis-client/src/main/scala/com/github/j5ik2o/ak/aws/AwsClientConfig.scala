package com.github.j5ik2o.ak.aws

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions

case class AwsClientConfig(region: Regions = Regions.AP_NORTHEAST_1,
                           credentialsProvider: Option[AWSCredentialsProvider] = None,
                           clientConfiguration: Option[ClientConfiguration] = None,
                           endpointConfiguration: Option[EndpointConfiguration] = None)
