package org.luvx.entity;

import lombok.*;
import org.luvx.common.utils.DateTimeUtils;

/**
 * @ClassName: org.luvx.common.entity
 * @Description:
 * @Author: Ren, Xie
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@Setter
public class LogEvent {
    private String ip;
    private Long   eventTime;
    private String timeZone;
    private String type;
    private String url;

    @Override
    public String toString() {
        return "LogEvent{" +
                "ip='" + ip + '\'' +
                ", eventTime=" + DateTimeUtils.epochMilli2Time(eventTime) +
                ", timeZone='" + timeZone + '\'' +
                ", type='" + type + '\'' +
                ", url='" + url + '\'' +
                '}';
    }

    public static LogEvent of(String[] tokens) {
        return LogEvent.builder()
                .ip(tokens[0]).eventTime(DateTimeUtils.str2EpochMilli(tokens[3])).timeZone(tokens[4])
                .type(tokens[5]).url(tokens[6])
                .build();
    }
}
