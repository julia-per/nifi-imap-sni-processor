package org.custom.processors;

import javax.mail.Flags;
import javax.mail.Message;
import javax.mail.search.*;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class IMAPFilterParser {

    private static final Pattern QUOTED_PATTERN = Pattern.compile("\"([^\"]*)\"");
    private static final Set<String> FORBIDDEN_CRITERIA = Set.of("BODY", "TEXT");

    public static SearchTerm parse(String filter, int startDepth, int maxDepth) throws IllegalArgumentException {
        if (filter == null || filter.trim().isEmpty()) {
            return null;
        }

        String trimmedFilter = filter.trim();
        String upperFilter = trimmedFilter.toUpperCase();

        boolean hasUID = containsUID(trimmedFilter);

        if (hasUID) {
            String withoutUID = removeUIDClause(trimmedFilter);
            if (withoutUID.trim().isEmpty()) {
                return null;
            }

            List<String> tokens = tokenize(withoutUID);
            if (tokens.isEmpty()) {
                return null;
            }
            org.custom.processors.IMAPFilterParser.ParseResult result = parseExpression(tokens, 0, startDepth, maxDepth);
            return result.term;
        }

        if (containsForbiddenCriteria(upperFilter)) {
            throw new IllegalArgumentException("""
                            BODY and TEXT search criteria are not allowed for performance reasons.
                            These would require downloading all messages for local filtering.
                            Use HEADER, FROM, TO, SUBJECT, or date criteria instead."""
            );
        }

        List<String> tokens = tokenize(trimmedFilter);
        org.custom.processors.IMAPFilterParser.ParseResult result = parseExpression(tokens, 0, startDepth, maxDepth);
        return result.term;
    }

    private static boolean containsUID(String filter) {
        if (filter == null) return false;
        return Pattern.compile("\\bUID\\b", Pattern.CASE_INSENSITIVE).matcher(filter).find();
    }

    private static String removeUIDClause(String filter) {
        return filter.replaceAll("(?i)\\bUID\\s+[\\d:*,\\-]+\\s*", "").trim();
    }

    private static boolean containsForbiddenCriteria(String upperFilter) {
        return upperFilter.contains(" BODY ") ||
                upperFilter.contains(" TEXT ") ||
                upperFilter.startsWith("BODY ") ||
                upperFilter.startsWith("TEXT ");
    }

    private static List<String> tokenize(String filter) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < filter.length(); i++) {
            char c = filter.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ' ' && !inQuotes) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            tokens.add(current.toString());
        }

        if (inQuotes) {
            throw new IllegalArgumentException("Unclosed quote in filter: " + filter);
        }

        return tokens;
    }

    private static org.custom.processors.IMAPFilterParser.ParseResult parseExpression(List<String> tokens, int pos, int depth, int maxDepth) {
        if (depth > maxDepth) {
            throw new IllegalArgumentException("Filter nesting depth exceeds limit of " + maxDepth);
        }

        List<SearchTerm> andTerms = new ArrayList<>();
        List<SearchTerm> orTerms = new ArrayList<>();
        boolean inOrSequence = false;

        while (pos < tokens.size()) {
            String token = tokens.get(pos);

            if (token.equalsIgnoreCase("OR")) {
                inOrSequence = true;
                pos++;
                continue;
            } else {
                org.custom.processors.IMAPFilterParser.ParseResult criterion = parseCriterion(tokens, pos);
                if (criterion.term == null) {
                    pos = criterion.nextPos;
                    continue;
                }

                if (inOrSequence) {
                    orTerms.add(criterion.term);
                } else {
                    andTerms.add(criterion.term);
                }
                pos = criterion.nextPos;
            }
        }

        SearchTerm result = null;

        if (!orTerms.isEmpty()) {
            if (orTerms.size() == 1) {
                result = orTerms.get(0);
            } else {
                SearchTerm combinedOr = orTerms.get(0);
                for (int i = 1; i < orTerms.size(); i++) {
                    combinedOr = new OrTerm(combinedOr, orTerms.get(i));
                }
                result = combinedOr;
            }
        }

        if (!andTerms.isEmpty()) {
            if (result == null) {
                if (andTerms.size() == 1) {
                    result = andTerms.get(0);
                } else {
                    result = new AndTerm(andTerms.toArray(new SearchTerm[0]));
                }
            } else {
                List<SearchTerm> allTerms = new ArrayList<>(andTerms);
                allTerms.add(result);
                result = new AndTerm(allTerms.toArray(new SearchTerm[0]));
            }
        }

        return new org.custom.processors.IMAPFilterParser.ParseResult(result, pos);
    }

    private static org.custom.processors.IMAPFilterParser.ParseResult parseCriterion(List<String> tokens, int pos) {
        if (pos >= tokens.size()) {
            return new org.custom.processors.IMAPFilterParser.ParseResult(null, pos);
        }

        String token = tokens.get(pos).toUpperCase();

        String rawToken = tokens.get(pos);
        if (rawToken.contains("\"")) {
            long quoteCount = rawToken.chars().filter(ch -> ch == '"').count();
            if (quoteCount % 2 != 0) {
                throw new IllegalArgumentException("Unclosed quote in filter: " + rawToken);
            }
        }

        if (FORBIDDEN_CRITERIA.contains(token)) {
            throw new IllegalArgumentException(
                    token + " search is not allowed for performance reasons. " +
                            "It would require downloading all messages for local filtering."
            );
        }

        try {
            switch (token) {
                case "ALL":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(null, pos + 1);

                case "SEEN":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.SEEN), true), pos + 1);

                case "UNSEEN":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.SEEN), false), pos + 1);

                case "ANSWERED":
                    return new org.custom.processors.IMAPFilterParser.ParseResult
                            (new FlagTerm(new Flags(Flags.Flag.ANSWERED), true), pos + 1);

                case "UNANSWERED":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.ANSWERED), false), pos + 1);

                case "DELETED":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.DELETED), true), pos + 1);

                case "UNDELETED":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.DELETED), false), pos + 1);

                case "DRAFT":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.DRAFT), true), pos + 1);

                case "UNDRAFT":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.DRAFT), false), pos + 1);

                case "FLAGGED":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.FLAGGED), true), pos + 1);

                case "UNFLAGGED":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.FLAGGED), false), pos + 1);

                case "RECENT":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(Flags.Flag.RECENT), true), pos + 1);

                case "NEW":
                    SearchTerm newTerm = new AndTerm(new SearchTerm[]{
                            new FlagTerm(new Flags(Flags.Flag.RECENT), true),
                            new FlagTerm(new Flags(Flags.Flag.SEEN), false)
                    });
                    return new org.custom.processors.IMAPFilterParser.ParseResult(newTerm, pos + 1);

                case "OLD":
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new NotTerm(new FlagTerm(new Flags(Flags.Flag.RECENT), true)), pos + 1);

                case "UID":
                    requireToken(tokens, pos + 1, "UID");
                    return new org.custom.processors.IMAPFilterParser.ParseResult(null, pos + 2);

                case "FROM":
                    requireToken(tokens, pos + 1, "FROM");
                    String fromValue = extractStringValue(tokens, pos + 1);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FromStringTerm(fromValue), pos + 2);

                case "TO":
                    requireToken(tokens, pos + 1, "TO");
                    String toValue = extractStringValue(tokens, pos + 1);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new RecipientStringTerm(Message.RecipientType.TO, toValue), pos + 2);

                case "CC":
                    requireToken(tokens, pos + 1, "CC");
                    String ccValue = extractStringValue(tokens, pos + 1);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new RecipientStringTerm(Message.RecipientType.CC, ccValue), pos + 2);

                case "BCC":
                    requireToken(tokens, pos + 1, "BCC");
                    String bccValue = extractStringValue(tokens, pos + 1);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new RecipientStringTerm(Message.RecipientType.BCC, bccValue), pos + 2);

                case "SUBJECT":
                    requireToken(tokens, pos + 1, "SUBJECT");
                    String subjectValue = extractStringValue(tokens, pos + 1);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new SubjectTerm(subjectValue), pos + 2);

                case "SINCE":
                    requireToken(tokens, pos + 1, "SINCE");
                    Date sinceDate = parseDate(extractStringValue(tokens, pos + 1));
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new ReceivedDateTerm(ComparisonTerm.GE, sinceDate), pos + 2);

                case "BEFORE":
                    requireToken(tokens, pos + 1, "BEFORE");
                    Date beforeDate = parseDate(extractStringValue(tokens, pos + 1));
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new ReceivedDateTerm(ComparisonTerm.LT, beforeDate), pos + 2);

                case "ON":
                    requireToken(tokens, pos + 1, "ON");
                    Date onDate = parseDate(extractStringValue(tokens, pos + 1));
                    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                    cal.setTime(onDate);
                    cal.set(Calendar.HOUR_OF_DAY, 0);
                    cal.set(Calendar.MINUTE, 0);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    Date start = cal.getTime();
                    cal.set(Calendar.HOUR_OF_DAY, 23);
                    cal.set(Calendar.MINUTE, 59);
                    cal.set(Calendar.SECOND, 59);
                    cal.set(Calendar.MILLISECOND, 999);
                    Date end = cal.getTime();
                    SearchTerm onTerm = new AndTerm(new SearchTerm[]{
                            new ReceivedDateTerm(ComparisonTerm.GE, start),
                            new ReceivedDateTerm(ComparisonTerm.LE, end)
                    });
                    return new org.custom.processors.IMAPFilterParser.ParseResult(onTerm, pos + 2);

                case "SENTSINCE":
                    requireToken(tokens, pos + 1, "SENTSINCE");
                    Date sentSinceDate = parseDate(extractStringValue(tokens, pos + 1));
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new SentDateTerm(ComparisonTerm.GE, sentSinceDate), pos + 2);

                case "SENTBEFORE":
                    requireToken(tokens, pos + 1, "SENTBEFORE");
                    Date sentBeforeDate = parseDate(extractStringValue(tokens, pos + 1));
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new SentDateTerm(ComparisonTerm.LT, sentBeforeDate), pos + 2);

                case "SENTON":
                    requireToken(tokens, pos + 1, "SENTON");
                    Date sentOnDate = parseDate(extractStringValue(tokens, pos + 1));
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new SentDateTerm(ComparisonTerm.EQ, sentOnDate), pos + 2);

                case "LARGER":
                    requireToken(tokens, pos + 1, "LARGER");
                    int largerSize = extractIntValue(tokens, pos + 1);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new SizeTerm(ComparisonTerm.GT, largerSize), pos + 2);

                case "SMALLER":
                    requireToken(tokens, pos + 1, "SMALLER");
                    int smallerSize = extractIntValue(tokens, pos + 1);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new SizeTerm(ComparisonTerm.LT, smallerSize), pos + 2);

                case "KEYWORD":
                    requireToken(tokens, pos + 1, "KEYWORD");
                    String keyword = extractStringValue(tokens, pos + 1);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(keyword), true), pos + 2);

                case "UNKEYWORD":
                    requireToken(tokens, pos + 1, "UNKEYWORD");
                    String unkeyword = extractStringValue(tokens, pos + 1);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new FlagTerm(new Flags(unkeyword), false), pos + 2);

                case "HEADER":
                    if (pos + 2 >= tokens.size()) {
                        throw new IllegalArgumentException("HEADER requires name and value");
                    }
                    String headerName = extractStringValue(tokens, pos + 1);
                    String headerValue = extractStringValue(tokens, pos + 2);
                    return new org.custom.processors.IMAPFilterParser.ParseResult(
                            new HeaderTerm(headerName, headerValue), pos + 3);

                default:
                    throw new IllegalArgumentException("Unknown or unsupported criterion: " + token);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing criterion '" + token + "': " + e.getMessage(), e);
        }
    }

    private static void requireToken(List<String> tokens, int pos, String criterion) {
        if (pos >= tokens.size()) {
            throw new IllegalArgumentException(criterion + " requires a value");
        }
    }

    private static String extractStringValue(List<String> tokens, int pos) {
        String value = tokens.get(pos);

        Matcher quotedMatcher = QUOTED_PATTERN.matcher(value);
        if (quotedMatcher.matches()) {
            return quotedMatcher.group(1);
        }

        return value;
    }

    private static int extractIntValue(List<String> tokens, int pos) {
        String value = extractStringValue(tokens, pos);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number: " + value);
        }
    }

    private static Date parseDate(String dateStr) {
        DateTimeFormatter[] formatters = {
                DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.US),
                DateTimeFormatter.ofPattern("d-MMM-yyyy", Locale.US),
                DateTimeFormatter.ofPattern("dd-MMM-yy", Locale.US),
                DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy"),
                DateTimeFormatter.ofPattern("MM/dd/yyyy"),
                DateTimeFormatter.ofPattern("dd MMM yyyy", Locale.US)
        };

        for (DateTimeFormatter formatter : formatters) {
            try {
                LocalDate localDate = LocalDate.parse(dateStr, formatter);
                return Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
            } catch (DateTimeParseException e) {
                // Try next format, ignore
            }
        }

        throw new IllegalArgumentException("Could not parse date: " + dateStr + ". Expected format like 01-Jan-2024");
    }

    private static class ParseResult {
        final SearchTerm term;
        final int nextPos;

        ParseResult(SearchTerm term, int nextPos) {
            this.term = term;
            this.nextPos = nextPos;
        }
    }
}