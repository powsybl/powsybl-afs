package com.powsybl.afs.server.jwt;

import org.junit.Test;

import java.security.Principal;

import static org.assertj.core.api.Assertions.assertThat;

public class AfsSecurityAuthenticationTest {

    @Test
    public void equalityTest() {
        AfsSecurityAuthentication afsSimpleSecurityContext = new AfsSecurityAuthentication("foo");
        Principal principal = (Principal) afsSimpleSecurityContext.getPrincipal();
        assertThat(principal.getName()).isEqualTo("foo");
        assertThat(afsSimpleSecurityContext.getPrincipal())
                .isEqualTo(afsSimpleSecurityContext.getPrincipal())
                .isEqualTo(new AfsSecurityAuthentication("foo").getPrincipal())
                .isNotEqualTo(new AfsSecurityAuthentication("bar").getPrincipal())
                .isNotEqualTo(null)
                .hasSameHashCodeAs(new AfsSecurityAuthentication("foo").getPrincipal());
        assertThat(afsSimpleSecurityContext.isAuthenticated()).isTrue();
        assertThat(afsSimpleSecurityContext.getDetails()).isNull();
        assertThat(afsSimpleSecurityContext.getAuthorities()).isNull();
        assertThat(afsSimpleSecurityContext.getCredentials()).isNull();
    }
}
