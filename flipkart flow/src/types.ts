/** Delivery address + GST details for one order, as used by FlipkartCheckout2. */
export interface AddressDetails {
  /** Contact name on the address card. */
  name: string;
  /** Business name — used as the address name and GST company name when set. */
  companyName?: string;
  /** 10-digit mobile saved on the address (set to the account's registered mobile). */
  mobile: string;
  pincode: string;
  /** Optional different pincode to use at checkout. */
  checkoutPincode?: string;
  /** Locality / area (Flipkart form field addressLine2). */
  locality: string;
  /** Full street address (Flipkart form field addressLine1). */
  addressLine1: string;
  city: string;
  /** Must match the option value in Flipkart's state dropdown, e.g. "Maharashtra". */
  state: string;
  addressType: "Home" | "Work";
  /** 15-character GSTIN. */
  gstNumber?: string;
}

/** Contents of order.json. */
export interface OrderConfig {
  productUrl: string;
  quantity: number;
  /** Phone number saved on the delivery address (the account mobile is only read and printed). */
  registeredMobile: string;
  address: Omit<AddressDetails, "mobile" | "gstNumber"> & { mobile?: string };
  gstNumber: string;
  /** GST invoice must be ticked with gstNumber (default true). */
  gstMandatory?: boolean;
  headless?: boolean;
  /** Playwright device name for mobile mode (default "Pixel 7"). */
  mobileDevice?: string;
}
