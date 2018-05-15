/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.proxy;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import static org.apache.nifi.proxy.ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE;

public class ProxyConfiguration {

    public static final ProxyConfiguration DIRECT_CONFIGURATION = new ProxyConfiguration();

    /**
     * This method can be used from customValidate method of components using this Controller Service
     * to validate the service is configured with the supported proxy types.
     * @param context the validation context
     * @param results if validation fails, an invalid validation result will be added to this collection
     * @param supportedProxyTypes specify supported proxy types, DIRECT is always supported
     */
    public static void validateProxyType(ValidationContext context, Collection<ValidationResult> results, Proxy.Type ... supportedProxyTypes) {
        if (context.getProperty(PROXY_CONFIGURATION_SERVICE).isSet()) {
            final ProxyConfigurationService proxyService = context.getProperty(PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
            final Proxy.Type proxyType = proxyService.getConfiguration().getProxyType();

            if (!proxyType.equals(Proxy.Type.DIRECT)
                    && Arrays.stream(supportedProxyTypes).noneMatch(supported -> supported.equals(proxyType))) {
                results.add(new ValidationResult.Builder()
                        .explanation(String.format("Proxy type %s is not supported.", proxyType))
                        .valid(false)
                        .subject(PROXY_CONFIGURATION_SERVICE.getDisplayName())
                        .build());
            }
        }
    }

    /**
     * A convenient method to get ProxyConfiguration instance from a PropertyContext.
     * @param context the process context
     * @return The proxy configurations at Controller Service if set, or DIRECT_CONFIGURATION
     */
    public static ProxyConfiguration getConfiguration(PropertyContext context) {
        return getConfiguration(context, () -> DIRECT_CONFIGURATION);
    }

    /**
     * This method can be used by Components those originally have per component proxy configurations
     * to implement ProxyConfiguration Controller Service with backward compatibility.
     * @param context the process context
     * @param perComponentSetting the function to supply ProxyConfiguration based on per component settings,
     *                            only called when Proxy Configuration Service is not set
     * @return The proxy configurations at Controller Service if set, or per component settings otherwise
     */
    public static ProxyConfiguration getConfiguration(PropertyContext context, Supplier<ProxyConfiguration> perComponentSetting) {
        if (context.getProperty(PROXY_CONFIGURATION_SERVICE).isSet()) {
            final ProxyConfigurationService proxyService = context.getProperty(PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
            return proxyService.getConfiguration();
        } else {
            return perComponentSetting.get();
        }
    }

    private Proxy.Type proxyType = Proxy.Type.DIRECT;
    private String proxyServerHost;
    private Integer proxyServerPort;
    private String proxyUserName;
    private String proxyUserPassword;

    public Proxy.Type getProxyType() {
        return proxyType;
    }

    public void setProxyType(Proxy.Type proxyType) {
        this.proxyType = proxyType;
    }

    public String getProxyServerHost() {
        return proxyServerHost;
    }

    public void setProxyServerHost(String proxyServerHost) {
        this.proxyServerHost = proxyServerHost;
    }

    public Integer getProxyServerPort() {
        return proxyServerPort;
    }

    public void setProxyServerPort(Integer proxyServerPort) {
        this.proxyServerPort = proxyServerPort;
    }

    public boolean hasCredential() {
        return proxyUserName != null && !proxyUserName.isEmpty();
    }

    public String getProxyUserName() {
        return proxyUserName;
    }

    public void setProxyUserName(String proxyUserName) {
        this.proxyUserName = proxyUserName;
    }

    public String getProxyUserPassword() {
        return proxyUserPassword;
    }

    public void setProxyUserPassword(String proxyUserPassword) {
        this.proxyUserPassword = proxyUserPassword;
    }

    /**
     * Create a Proxy instance based on proxy type, proxy server host and port.
     */
    public Proxy createProxy() {
        return Proxy.Type.DIRECT.equals(proxyType) ? Proxy.NO_PROXY : new Proxy(proxyType, new InetSocketAddress(proxyServerHost, proxyServerPort));
    }

}
